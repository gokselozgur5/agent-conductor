"""Configured chat execution remains explicit and uses private profile files."""
import argparse
import json
from pathlib import Path
import runpy
import tempfile
import unittest


class OperatorConversationTests(unittest.TestCase):
    def test_configured_profiles_require_dispatch_and_private_files(self):
        arguments = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'tools/conductor'))['conversation_arguments']
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / 'agents.json'
            profile.write_text(json.dumps([{'id': 'codex'}]))
            profile.chmod(0o600)
            config = {'conversation_agents': str(profile)}
            args = argparse.Namespace(conversation_agents=None, dispatch=False)
            with self.assertRaisesRegex(ValueError, 'require --dispatch'):
                arguments(config, args)
            args.dispatch = True
            self.assertEqual(arguments(config, args), ['--conversation-agents', str(profile)])
            profile.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'private regular file'):
                arguments(config, args)
            self.assertEqual(arguments({}, args), [])

    def test_explicit_profile_selection_overrides_saved_selection(self):
        arguments = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'tools/conductor'))['conversation_arguments']
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / 'explicit.json'
            profile.write_text('[]')
            profile.chmod(0o600)
            args = argparse.Namespace(conversation_agents=profile, dispatch=True)
            self.assertEqual(arguments({'conversation_agents': '/missing'}, args), ['--conversation-agents', str(profile)])
