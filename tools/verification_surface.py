"""Verify a selected frozen snapshot against its durable source identity."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time

from native_observation_guard import protected_digest, require


def verify_snapshot(root, expected, deadline):
    root = Path(root)
    require(root.is_absolute() and root.resolve(strict=True)==root, 'verification_root_invalid')
    require(isinstance(expected,str) and re.fullmatch('snapshot:[0-9a-f]{64}',expected), 'verification_source_invalid')
    manifest = root/'manifest.json'
    descriptor = os.open(manifest,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        metadata=os.fstat(stream.fileno())
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink==1 and metadata.st_size<=4*1024**2, 'verification_manifest_invalid')
        raw=stream.read(4*1024**2+1)
    require(len(raw)<=4*1024**2 and 'snapshot:'+hashlib.sha256(raw).hexdigest()==expected, 'verification_manifest_mismatch')
    value=json.loads(raw)
    require(isinstance(value,dict) and set(value)=={'version','entries'} and type(value['version']) is int and value['version']==1, 'verification_manifest_invalid')
    entries=value['entries']
    require(isinstance(entries,list) and len(entries)<=10000, 'verification_entry_limit')
    tree=root/'tree'
    require(tree.resolve(strict=True)==tree and tree.is_dir(), 'verification_tree_invalid')
    wanted=set();previous=None;total=0
    for entry in entries:
        require(time.monotonic()<=deadline, 'verification_deadline')
        require(isinstance(entry,dict) and set(entry)=={'path','sha256','bytes','executable'}, 'verification_entry_invalid')
        name=entry['path'];digest=entry['sha256'];size=entry['bytes'];executable=entry['executable']
        require(isinstance(name,str) and name and not name.startswith('/') and all(p not in ('','.','..','.git') for p in name.split('/')), 'verification_path_invalid')
        require(str(PurePosixPath(name))==name and (previous is None or previous<name), 'verification_path_order')
        previous=name
        require(type(size) is int and 0<=size<=32*1024**2 and type(executable) is bool, 'verification_entry_invalid')
        path=tree/name
        require(path.resolve()==path, 'verification_link_or_escape')
        if digest is None:
            require(size==0 and not executable and not path.exists(), 'verification_deletion_changed')
            continue
        require(isinstance(digest,str) and re.fullmatch('[0-9a-f]{64}',digest), 'verification_entry_invalid')
        wanted.add(name);total+=size
        require(total<=64*1024**2, 'verification_byte_limit')
        descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(descriptor,'rb') as stream:
            before=os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_size==size and bool(before.st_mode&0o111)==executable, 'verification_file_metadata_changed')
            content=hashlib.sha256();count=0
            while True:
                chunk=stream.read(65536)
                if not chunk:break
                count+=len(chunk)
                require(count<=size and time.monotonic()<=deadline, 'verification_file_bound')
                content.update(chunk)
            after=os.fstat(stream.fileno())
        require(count==size and content.hexdigest()==digest and (before.st_size,before.st_mtime_ns,before.st_ctime_ns)==(after.st_size,after.st_mtime_ns,after.st_ctime_ns), 'verification_file_changed')
    observed=set();count=0
    for directory,dirs,files in os.walk(tree,followlinks=False):
        for name in dirs+files:
            count+=1
            require(count<=10000 and time.monotonic()<=deadline, 'verification_inventory_limit')
            path=Path(directory)/name;metadata=path.lstat()
            require(stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode), 'verification_special_file')
            if stat.S_ISREG(metadata.st_mode):observed.add(str(path.relative_to(tree)))
    require(observed==wanted, 'verification_inventory_changed')
    return protected_digest(tree,deadline)
