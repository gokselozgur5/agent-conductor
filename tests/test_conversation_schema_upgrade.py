"""Preserve existing native execution identities when adding room recipients."""
import json
import pathlib
import sqlite3
import unittest


class ConversationSchemaUpgrade(unittest.TestCase):
    def test_existing_launches_and_jobs_survive_recipient_migration(self):
        migrations = pathlib.Path(__file__).resolve().parents[1] / 'crates/store/migrations'
        with sqlite3.connect(':memory:') as db:
            db.execute('PRAGMA foreign_keys=ON')
            for number in (33, 34, 35):
                db.executescript(next(migrations.glob(f'{number:04d}_*.sql')).read_text())
            db.execute("INSERT INTO conversations VALUES ('room','/project',1)")
            db.execute("INSERT INTO conversation_messages(id,conversation_id,sender,body,created_at_ms) VALUES ('request','room','operator','work',2)")
            db.execute("INSERT INTO conversation_turns VALUES ('request','room','codex','original-launch',?,?,NULL,NULL,'unknown',3)", ('a' * 64, 'b' * 64))
            profile = json.dumps({'id': 'codex'})
            db.execute("INSERT INTO conversation_jobs VALUES ('request',?,'queued',2)", (profile,))
            before = db.execute('SELECT * FROM conversation_turns').fetchall()
            for number in (36, 37, 38, 39, 40):
                db.executescript(next(migrations.glob(f'{number:04d}_*.sql')).read_text())
            self.assertEqual(db.execute('SELECT * FROM conversation_turns').fetchall(), before)
            self.assertEqual(db.execute('SELECT agent_id,agent_profile_json FROM conversation_jobs').fetchall(), [('codex', profile)])
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
            db.execute("INSERT INTO conversation_jobs VALUES ('request','claude',?,'queued',4)", (json.dumps({'id': 'claude'}),))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM conversation_jobs').fetchone()[0], 2)
            # Unknown native outcome still excludes another room launch.
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("INSERT INTO conversation_turns VALUES ('request','room','claude','second-launch',?,?,NULL,NULL,'launch_intent',4)", ('a' * 64, 'b' * 64))


if __name__ == '__main__':
    unittest.main()
