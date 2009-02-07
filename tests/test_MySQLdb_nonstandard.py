import unittest

import MySQLdb


class NonStandard(unittest.TestCase):
    """Test _mysql.connection internals."""

    def setUp(self):
        self.conn = MySQLdb.connect(db='test')

    def tearDown(self):
        self.conn.close()

    def test_thread_id(self):
        tid = self.conn._db.thread_id()
        self.assertTrue(isinstance(tid, int),
                        "thread_id didn't return an int.")

        self.assertRaises(TypeError, self.conn._db.thread_id, ('evil',),
                          "thread_id shouldn't accept arguments.")

    def test_affected_rows(self):
        self.assertEquals(self.conn._db.affected_rows(), 0,
                          "Should return 0 before we do anything.")


    def test_debug(self):
        # FIXME Only actually tests if you lack SUPER
        self.assertRaises(MySQLdb.OperationalError,
                          self.conn._db.dump_debug_info)

    def test_charset_name(self):
        self.assertTrue(isinstance(self.conn._db.character_set_name(), str),
                        "Should return a string.")

    def test_host_info(self):
        self.assertTrue(isinstance(self.conn._db.get_host_info(), str),
                        "Should return a string.")

    def test_proto_info(self):
        self.assertTrue(isinstance(self.conn._db.get_proto_info(), int),
                        "Should return an int.")

    def test_server_info(self):
        self.assertTrue(isinstance(self.conn._db.get_server_info(), str),
                        "Should return an str.")

