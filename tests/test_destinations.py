import unittest

from dispatch.destinations import DestinationPolicy, parse_destination


class DestinationPolicyTests(unittest.TestCase):
    def test_default_policy_denies_destinations(self):
        self.assertFalse(DestinationPolicy().allows("https://example.com/hook"))

    def test_normalized_origins_match_paths_and_queries(self):
        policy = DestinationPolicy(["https://EXAMPLE.com:443/", "https://other.example:8443"])
        self.assertTrue(policy.allows("https://example.com/hook?order=1"))
        self.assertTrue(policy.allows("https://other.example:8443/hook"))
        self.assertFalse(policy.allows("https://other.example/hook"))

    def test_private_and_ambiguous_addresses_are_rejected(self):
        for host in ["127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "[::1]", "[fc00::1]", "224.0.0.1", "[ff02::1]", "[fec0::1]", "[::ffff:127.0.0.1]", "127.1", "2130706433", "0177.0.0.1", "localhost", "app.localhost", "app.local", "app.internal"]:
            with self.subTest(host=host):
                with self.assertRaises(ValueError):
                    parse_destination("https://" + host + "/hook")

    def test_malformed_or_credentialed_urls_are_rejected(self):
        for value in ["http://example.com", "https://user@example.com", "https://example.com#", "https://example.com\\@other.example", "https://example.com\n/hook", "https://%65xample.com", "https://example.com.", "https://example..com", "https://-example.com", "https://example.com:0", "https://example.com:99999", "https://[invalid]", "https://exämple.com"]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    parse_destination(value)

    def test_origin_configuration_rejects_paths_queries_and_private_ips(self):
        for value in ["https://example.com/hook", "https://example.com?x=1", "https://127.0.0.1"]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    DestinationPolicy([value])

    def test_public_ip_origin_requires_exact_address(self):
        policy = DestinationPolicy(["https://8.8.8.8", "https://[2606:4700:4700::1111]"])
        self.assertTrue(policy.allows("https://8.8.8.8/hook"))
        self.assertTrue(policy.allows("https://[2606:4700:4700:0:0:0:0:1111]/hook"))
        self.assertFalse(policy.allows("https://8.8.4.4/hook"))


if __name__ == "__main__":
    unittest.main()
