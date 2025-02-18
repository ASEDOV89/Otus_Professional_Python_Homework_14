import unittest

from memc_load import parse_appsinstalled


class TestMemcLoad(unittest.TestCase):
    def test_parse_appsinstalled_valid(self):
        line = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567"
        result = parse_appsinstalled(line)
        self.assertIsNotNone(result)
        self.assertEqual(result.dev_type, "idfa")
        self.assertEqual(result.dev_id, "1rfw452y52g2gq4g")
        self.assertEqual(result.lat, 55.55)
        self.assertEqual(result.lon, 42.42)
        self.assertEqual(result.apps, [1423, 43, 567])

    def test_parse_appsinstalled_invalid(self):
        line = "idfa\t\t55.55\t42.42\t1423,43,567"
        result = parse_appsinstalled(line)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
