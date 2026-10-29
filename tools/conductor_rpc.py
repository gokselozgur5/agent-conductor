"""Control-plane access from another OS account using an operator credential."""
import http.client
import ipaddress
import json
import uuid

LIMIT = 1024 * 1024


def send(address, token, payload):
    """Send once, without redirects, proxies, environment credentials or retries."""
    host, port = address.rsplit(':', 1)
    host = host.strip('[]')
    if not ipaddress.ip_address(host).is_loopback or not 1 <= int(port) <= 65535:
        raise ValueError('RPC requires a numeric loopback address and valid port')
    if not 32 <= len(token) <= 256 or any(not 33 <= ord(c) <= 126 for c in token):
        raise ValueError('invalid operator credential')
    request = dict(protocol='agent-conductor.local', version=1,
                   message_id=str(uuid.uuid4()), correlation_id=str(uuid.uuid4()),
                   payload=payload)
    body = json.dumps(request).encode()
    if len(body) > LIMIT:
        raise ValueError('request exceeds frame limit')
    connection = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        connection.request('POST', '/api/rpc', body,
                           {'Authorization': 'Bearer ' + token,
                            'Content-Type': 'application/json'})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError(f'RPC refused with HTTP {response.status}')
        data = response.read(LIMIT + 1)
        if len(data) > LIMIT:
            raise ValueError('response exceeds frame limit')
        result = json.loads(data)
        if (result.get('protocol') != request['protocol'] or result.get('version') != 1
                or result.get('request_message_id') != request['message_id']
                or result.get('correlation_id') != request['correlation_id']):
            raise ValueError('RPC response does not match request')
        return result
    except http.client.HTTPException:
        raise ValueError('invalid HTTP response; reconcile before retrying') from None
    finally:
        connection.close()


def exit_code(response):
    result = response['result']
    if result['status'] == 'ok':
        return 0
    return {'validation': 2, 'unsupported_protocol': 2, 'conflict': 3,
            'unavailable': 4, 'invariant_violation': 5, 'internal': 6}.get(result['payload']['code'], 6)
