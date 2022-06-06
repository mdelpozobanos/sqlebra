class Test_DictInterface:

    @classmethod
    def setUpClass(cls):
        cls.dbdict = {'x': True, 'y': 1, 'z': 'Test'}
        return ('setitem', 'getitem', 'contains', 'len', 'edititem', 'additem', 'delitem', 'obj_get', 'py_get')

    # Test public interface
    # ------------------------------------------------------------------------------------------------------------------

    def setitem(self):
        for k, v in self.dbdict.items():
            self.db[k] = v

    def getitem(self):
        for k, v in self.dbdict.items():
            self.assertEqual(v, self.db[k])

    def contains(self):
        for k in self.dbdict.keys():
            self.assertTrue(k in self.db)

    def len(self):
        self.assertEqual(len(self.dbdict), len(self.db))

    def edititem(self):
        self.dbdict['x'] = False
        self.db['x'] = False
        self.assertEqual(self.dbdict, self.db)

    def additem(self):
        self.dbdict['k'] = False
        self.db['k'] = False
        self.assertEqual(self.dbdict, self.db)

    def delitem(self):
        with self.subTest(delete='only use object'):
            del self.dbdict['y']
            del self.db['y']
            self.assertEqual(self.dbdict, self.db)
        with self.subTest(delete='multy use object'):
            del self.dbdict['x']
            del self.db['x']
            self.assertEqual(self.dbdict, self.db)

    def obj_get(self):
        self.assertDictEqual(self.dbdict, dict(tuple((k, v.py) for k, v in self.db.objs.items())))

    def py_get(self):
        self.assertDictEqual(self.dbdict, self.db.py)

