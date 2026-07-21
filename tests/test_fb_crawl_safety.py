import json
import subprocess
import random
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scraper import fb_keyword_nightly
from scraper import fb_reel_night_cron


PAGE_NAVIGATE_METHOD = "Page" + "." + "navigate"


class FakeCDP:
    """Minimal fake CDP client used for navigation safety tests."""

    def __init__(self, runtime_evaluate_responses=None):
        self.send_calls = []
        self.runtime_evaluate_responses = list(runtime_evaluate_responses or [])

    def send(self, method, params=None):
        self.send_calls.append((method, params))
        if method == "Runtime.evaluate" and self.runtime_evaluate_responses:
            return self.runtime_evaluate_responses.pop(0)
        return {}


def _fake_now(values):
    values = list(values)
    state = {"i": 0}
    if len(values) > 1:
        step = float(values[-1]) - float(values[-2])
        if step <= 0:
            step = 0.1
    else:
        step = 0.1

    def now():
        i = state["i"]
        state["i"] = i + 1
        if i >= len(values):
            if not values:
                return 0.0 + step * i
            return float(values[-1]) + (step * (i - len(values) + 1))
        return float(values[i])

    return now


class TestSafetyDetection(unittest.TestCase):
    """Pure safety state detection tests."""

    def test_login_warning_detection(self):
        state = {
            "url": "https://www.facebook.com/login",
            "title": "Log in to Facebook",
            "body_text": "Please log in to your Facebook account to continue.",
        }
        self.assertEqual(
            fb_keyword_nightly.parse_facebook_safety_reason(state),
            fb_keyword_nightly.SAFETY_REASON_LOGIN,
        )

    def test_checkpoint_warning_detection(self):
        state = {
            "url": "https://www.facebook.com/checkpoint/",
            "title": "Checkpoint",
            "body_text": "Your account checkpoint was triggered.",
        }
        self.assertEqual(
            fb_keyword_nightly.parse_facebook_safety_reason(state),
            fb_keyword_nightly.SAFETY_REASON_CHECKPOINT,
        )

    def test_captcha_warning_detection(self):
        state = {
            "url": "https://www.facebook.com",
            "title": "Facebook",
            "body_text": "Please verify you are not a robot before continuing.",
        }
        self.assertEqual(
            fb_keyword_nightly.parse_facebook_safety_reason(state),
            fb_keyword_nightly.SAFETY_REASON_CAPTCHA,
        )

    def test_action_block_warning_detection(self):
        state = {
            "url": "https://www.facebook.com",
            "title": "Action blocked",
            "body_text": "This action is temporarily blocked on Facebook.",
        }
        self.assertEqual(
            fb_keyword_nightly.parse_facebook_safety_reason(state),
            fb_keyword_nightly.SAFETY_REASON_ACTION_BLOCK,
        )

    def test_auth_challenge_warning_detection(self):
        state = {
            "url": "https://www.facebook.com/",
            "title": "Facebook account verification",
            "body_text": "This security check asks you to review unusual activity and verify your identity.",
        }
        self.assertEqual(
            fb_keyword_nightly.parse_facebook_safety_reason(state),
            fb_keyword_nightly.SAFETY_REASON_AUTH_CHALLENGE,
        )


class TestNavigationSafetyPolling(unittest.TestCase):
    """Navigation polling + warning detection tests."""

    def test_delayed_checkpoint_warning_detected(self):
        page_states = [
            {
                "url": "https://www.facebook.com/",
                "ready_state": "complete",
                "title": "",
                "body_text": "",
            },
            {
                "url": "https://www.facebook.com/checkpoint/",
                "ready_state": "complete",
                "title": "Checkpoint required",
                "body_text": "This checkpoint was triggered on your account.",
            },
        ]
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6])

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {"url": "https://www.facebook.com/"}

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/search/videos/?q=test",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"], fb_keyword_nightly.SAFETY_REASON_CHECKPOINT
        )
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )

    def test_delayed_captcha_warning_detected(self):
        page_states = [
            {
                "url": "https://www.facebook.com/",
                "ready_state": "loading",
                "title": "",
                "body_text": "",
            },
            {
                "url": "https://www.facebook.com/",
                "ready_state": "complete",
                "title": "",
                "body_text": "Please verify you are not a robot before continuing.",
            },
        ]
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6])

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {"url": "https://www.facebook.com/"}

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/search/videos/?q=abc",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"], fb_keyword_nightly.SAFETY_REASON_CAPTCHA
        )
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )

    def test_navigation_verify_timeout_fail_closed(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.5, 1.1, 1.1])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        def page_state_fn(_):
            return {
                "url": "https://www.facebook.com/",
                "ready_state": "loading",
                "title": "loading",
                "body_text": "",
            }

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/search/videos/?q=timeout",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=1.0,
            poll_interval_seconds=0.5,
        )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT,
        )

    def test_navigation_verify_timeout_records_bounded_redacted_evidence(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8])

        def page_state_fn(_):
            return {
                "url": "https://www.facebook.com/search/videos/?q=private+token+value&src=feed",
                "ready_state": "loading",
                "title": "",
                "body_text": "this should not be emitted to diagnostics",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        with patch.object(fb_keyword_nightly, "MAX_NAVIGATION_VERIFY_SAMPLES", 3):
            ok = fb_keyword_nightly.navigate_with_safety(
                cdp,
                "https://www.facebook.com/search/videos/?q=private+token+value",
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=page_state_fn,
                timeout_seconds=1.8,
                poll_interval_seconds=0.2,
            )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT,
        )
        evidence = state.get("navigation_verification") or {}
        self.assertEqual(evidence.get("outcome"), fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT)
        samples = evidence.get("samples", [])
        self.assertLessEqual(len(samples), 3)
        self.assertGreater(len(samples), 0)
        final = evidence.get("final")
        self.assertIsInstance(final, dict)
        self.assertEqual(final, samples[-1])

        first = samples[0]
        self.assertTrue(isinstance(first.get("poll_ordinal"), int))
        self.assertTrue(isinstance(first.get("elapsed_ms"), (int, float)))
        self.assertIn("url", first)
        self.assertIn("ready_state", first)
        self.assertIn("payload_usable", first)
        self.assertIn("destination_match", first)
        self.assertIn("approved_host", first["url"])
        self.assertEqual(first["url"]["approved_host"], True)
        self.assertTrue(first["payload_usable"])
        self.assertFalse("body_text" in first["url"])
        self.assertFalse("body_text" in first)

    def test_navigation_verify_timeout_bounded_by_max_samples(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6])

        def page_state_fn(_):
            return {
                "url": "https://www.facebook.com/search/videos/?q=timeout",
                "ready_state": "loading",
                "title": "",
                "body_text": "hidden",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        with patch.object(fb_keyword_nightly, "MAX_NAVIGATION_VERIFY_SAMPLES", 2):
            ok = fb_keyword_nightly.navigate_with_safety(
                cdp,
                "https://www.facebook.com/search/videos/?q=timeout",
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=page_state_fn,
                timeout_seconds=2.0,
                poll_interval_seconds=0.2,
            )
        self.assertFalse(ok)
        evidence = state["navigation_verification"]
        self.assertEqual(evidence["max_samples"], 2)
        self.assertEqual(len(evidence["samples"]), 2)
        self.assertEqual(evidence["samples"][-1], evidence["final"])
        self.assertNotIn("title", evidence["samples"][-1])
        self.assertNotIn("body_text", evidence["samples"][-1])

    def test_navigation_verify_evidence_redacts_query_values_and_path(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8])

        page_states = [
            {
                "url": "https://www.facebook.com/reel/123456789012345678901234567890?__a=1&foo=secret-token",
                "ready_state": "loading",
                "title": "",
                "body_text": "forbidden text",
            },
            {
                "url": "https://www.facebook.com/reel/123456789012345678901234567890?__a=1&foo=secret-token",
                "ready_state": "loading",
                "title": "",
                "body_text": "forbidden text",
            },
            {
                "url": "https://www.facebook.com/reel/123456789012345678901234567890",
                "ready_state": "loading",
                "title": "",
                "body_text": "forbidden text",
            },
        ]

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {
                "url": "https://www.facebook.com/reel/123456789012345678901234567890",
                "ready_state": "loading",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/reel/99999999",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=0.8,
            poll_interval_seconds=0.2,
        )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT,
        )

        evidence = state["navigation_verification"]
        samples = evidence["samples"]
        self.assertGreaterEqual(len(samples), 1)
        flattened_keys = sorted(evidence["samples"][-1]["url"].keys())
        self.assertIn("query_keys", flattened_keys)
        self.assertIn("path_shape", flattened_keys)
        self.assertEqual(evidence["samples"][-1]["url"]["host"], "www.facebook.com")
        self.assertIn("<id>", evidence["samples"][-1]["url"]["path_shape"])
        self.assertNotIn("secret-token", evidence["samples"][-1]["url"]["path_shape"])
        self.assertNotIn("=", repr(evidence["samples"][-1]["url"]))
        self.assertNotIn("body_text", evidence["samples"][-1]["url"])

    def test_navigation_verify_success_sample_is_redacted(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8])
        target = "https://www.facebook.com/reel/123456?__a=1&token=private-token-value"
        page_states = [
            {
                "url": "https://www.facebook.com/search/videos/?q=other",
                "ready_state": "loading",
                "title": "",
                "body_text": "forbidden title",
            },
            {
                "url": "https://www.facebook.com/watch/?v=123456",
                "ready_state": "complete",
                "title": "Watch page title",
                "body_text": "forbidden body",
            },
        ]

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {
                "url": "https://www.facebook.com/watch/?v=123456",
                "ready_state": "complete",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.2,
        )
        self.assertTrue(ok)
        self.assertEqual(state["stop_reason"], "")
        evidence = state["navigation_verification"]
        final = evidence["final"]
        self.assertEqual(final["destination_match"], True)
        self.assertTrue(final["payload_usable"])
        self.assertNotIn("title", final)
        self.assertNotIn("body_text", final)
        self.assertNotIn("title", final["url"])
        self.assertNotIn("body_text", final["url"])
        self.assertIn("query_keys", final["url"])
        self.assertIn("ready_state", evidence["samples"][-1])
        self.assertTrue("payload_usable" in evidence["samples"][-1])

    def test_navigation_verify_success_records_destination_match(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8])
        target = "https://www.facebook.com/reel/123456"
        page_states = [
            {
                "url": "https://www.facebook.com/search/videos/?q=final",
                "ready_state": "loading",
                "title": "",
                "body_text": "",
            },
            {
                "url": "https://www.facebook.com/watch/?v=123456",
                "ready_state": "complete",
                "title": "",
                "body_text": "",
            },
        ]

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {
                "url": "https://www.facebook.com/watch/?v=123456",
                "ready_state": "complete",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.2,
        )
        self.assertTrue(ok)
        self.assertEqual(state["stop_reason"], "")
        evidence = state["navigation_verification"]
        self.assertEqual(evidence["outcome"], "verified")
        self.assertEqual(evidence["target"]["host"], "www.facebook.com")
        self.assertGreaterEqual(len(evidence["samples"]), 2)
        self.assertEqual(evidence["samples"][-1]["ready_state"], "complete")
        self.assertTrue(evidence["samples"][-1]["destination_match"])
        self.assertEqual(evidence["final"], evidence["samples"][-1])

    def test_navigation_video_reel_watch_alias_verified_without_false_timeout(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6])

        def page_state_fn(_):
            return {
                "url": "https://www.facebook.com/watch/?v=123456",
                "ready_state": "complete",
                "title": "",
                "body_text": "",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/reel/123456/",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertTrue(ok)
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )

    def test_navigation_cross_alias_www_and_m_hosts_is_accepted(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6])

        def page_state_fn(_):
            return {
                "url": "https://m.facebook.com/watch/?v=123456",
                "ready_state": "complete",
                "title": "",
                "body_text": "",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/reel/123456/",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertTrue(ok)
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )

    def test_navigation_verification_defaults_to_safety_page_state(self):
        target_url = "https://www.facebook.com/watch/?v=123456"
        runtime_state = {
            "result": {
                "value": {
                    "url": target_url,
                    "title": "",
                    "ready_state": "complete",
                    "body_text": "",
                }
            }
        }
        cdp = FakeCDP(runtime_evaluate_responses=[runtime_state])
        now = _fake_now([0.0, 0.2, 0.4, 0.6])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target_url,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            timeout_seconds=1.5,
            poll_interval_seconds=0.2,
        )

        self.assertTrue(ok)
        self.assertEqual(state["stop_reason"], "")
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == "Runtime.evaluate"]),
            1,
        )

        probe_cdp = FakeCDP(runtime_evaluate_responses=[runtime_state])
        observed_state = fb_keyword_nightly._safety_page_state(probe_cdp)
        self.assertEqual(observed_state.get("ready_state"), "complete")
        self.assertEqual(observed_state.get("url"), target_url)

    def test_navigation_verification_rejects_missing_ready_state(self):
        target_url = "https://www.facebook.com/watch/?v=123456"
        cdp = FakeCDP(
            runtime_evaluate_responses=[
                {
                    "result": {
                        "value": {
                            "url": target_url,
                            "title": "",
                            "body_text": "",
                        }
                    }
                }
            ]
        )
        now = _fake_now([0.0, 0.2, 0.4, 0.6])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target_url,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            timeout_seconds=1.0,
            poll_interval_seconds=0.2,
        )

        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT,
        )

    def test_navigation_verification_rejects_malformed_runtime_state_shape(self):
        target_url = "https://www.facebook.com/watch/?v=123456"
        cdp = FakeCDP(
            runtime_evaluate_responses=[
                {"unexpected": {"shape": True}},
            ]
        )
        now = _fake_now([0.0, 0.2, 0.4, 0.6])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target_url,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            timeout_seconds=1.0,
            poll_interval_seconds=0.2,
        )

        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT,
        )

    def test_adapter_navigation_verification_accepts_double_nested_runtime_shape(self):
        target_url = "https://www.facebook.com/watch/?v=123456"
        cdp = FakeCDP(
            runtime_evaluate_responses=[
                {
                    "result": {
                        "result": {
                            "value": {
                                "url": target_url,
                                "title": "",
                                "ready_state": "complete",
                                "body_text": "",
                            }
                        }
                    }
                }
            ]
        )
        now = _fake_now([0.0, 0.2, 0.4, 0.6])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target_url,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            timeout_seconds=1.0,
            poll_interval_seconds=0.2,
        )

        self.assertTrue(ok)
        self.assertEqual(
            state["stop_reason"],
            "",
        )

    def test_adapter_navigation_verification_accepts_flat_runtime_shape(self):
        target_url = "https://www.facebook.com/watch/?v=123456"
        cdp = FakeCDP(
            runtime_evaluate_responses=[
                {
                    "value": {
                        "url": target_url,
                        "title": "",
                        "ready_state": "complete",
                        "body_text": "",
                    }
                }
            ]
        )
        now = _fake_now([0.0, 0.2, 0.4, 0.6])
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )

        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target_url,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            timeout_seconds=1.0,
            poll_interval_seconds=0.2,
        )

        self.assertTrue(ok)
        self.assertEqual(
            state["stop_reason"],
            "",
        )

    def test_navigation_same_media_id_rejected_on_non_facebook_host(self):
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": "https://facebook.com.evil/reel/123456",
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                "https://www.facebook.com/watch/?v=123456",
            )
        )

    def test_navigation_same_host_with_mismatched_media_id_rejected(self):
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": "https://www.facebook.com/reel/999999",
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                "https://www.facebook.com/watch/?v=123456",
            )
        )

    def test_navigation_reached_accepts_profile_php_reels_tab_to_canonical_reels(self):
        target = "https://www.facebook.com/profile.php?id=100047211517264&sk=reels_tab"
        current = "https://www.facebook.com/drericberg/reels/"
        self.assertTrue(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": current,
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                target,
            )
        )

    def test_navigation_reached_rejects_profile_php_wrong_tab(self):
        target = "https://www.facebook.com/profile.php?id=100047211517264&sk=all_posts"
        current = "https://www.facebook.com/drericberg/reels/"
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": current,
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                target,
            )
        )

    def test_navigation_reached_rejects_profile_php_reels_tab_to_unrelated_destination(self):
        target = "https://www.facebook.com/profile.php?id=100047211517264&sk=reels_tab"
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": "https://www.facebook.com/drericberg/about",
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                target,
            )
        )

    def test_navigation_reached_rejects_profile_php_reels_tab_with_deceptive_host(self):
        target = "https://www.facebook.com/profile.php?id=100047211517264&sk=reels_tab"
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": "https://facebook.com.evil/drericberg/reels/",
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "",
                },
                target,
            )
        )

    def test_navigation_reached_rejects_incomplete_document_for_profile_php_redirect(self):
        target = "https://www.facebook.com/profile.php?id=100047211517264&sk=reels_tab"
        self.assertFalse(
            fb_keyword_nightly._is_navigation_destination_reached(
                {
                    "url": "https://www.facebook.com/drericberg/reels/",
                    "ready_state": "loading",
                    "title": "",
                    "body_text": "",
                },
                target,
            )
        )

    def test_stopped_state_prevents_second_navigation(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 1.2, 1.3])

        def warning_page_state_fn(_):
            return {
                "url": "https://www.facebook.com/checkpoint/",
                "title": "checkpoint",
                "body_text": "checkpoint required",
            }

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/search/videos/?q=first",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=warning_page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertFalse(ok)
        navigations_after_first = len(
            [m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]
        )
        self.assertEqual(navigations_after_first, 1)

        fb_keyword_nightly.navigate_with_safety(
            cdp,
            "https://www.facebook.com/search/videos/?q=second",
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
                page_state_fn=lambda _: {
                    "url": "https://www.facebook.com/search/videos/?q=second",
                    "ready_state": "complete",
                    "title": "",
                    "body_text": "ok",
                },
                timeout_seconds=2.0,
                poll_interval_seconds=0.1,
            )
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            navigations_after_first,
        )

    def test_navigation_budget_enforced_in_navigation_helper(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.1, 0.2, 0.3, 0.4])
        first_target = "https://www.facebook.com/search/videos/?q=first"
        second_target = "https://www.facebook.com/search/videos/?q=second"
        third_target = "https://www.facebook.com/search/videos/?q=third"

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=2,
            now_ts=0.0,
        )
        self.assertTrue(
            fb_keyword_nightly.navigate_with_safety(
                cdp,
                first_target,
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=lambda _, target_url=first_target: {
                    "url": target_url,
                    "ready_state": "complete",
                },
                timeout_seconds=2.0,
                poll_interval_seconds=0.1,
            )
        )
        self.assertEqual(state["navigation_count"], 1)
        self.assertTrue(
            fb_keyword_nightly.navigate_with_safety(
                cdp,
                second_target,
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=lambda _, target_url=second_target: {
                    "url": target_url,
                    "ready_state": "complete",
                },
                timeout_seconds=2.0,
                poll_interval_seconds=0.1,
            )
        )
        self.assertEqual(state["navigation_count"], 2)

        self.assertFalse(
            fb_keyword_nightly.navigate_with_safety(
                cdp,
                third_target,
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=lambda _, target_url=third_target: {
                    "url": target_url,
                    "ready_state": "complete",
                },
                timeout_seconds=2.0,
                poll_interval_seconds=0.1,
            )
        )
        self.assertEqual(state["navigation_count"], 2)
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            2,
        )
        self.assertEqual(
            state["stop_reason"],
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_LIMIT,
        )

    def test_matching_url_is_not_verified_until_ready(self):
        target = "https://www.facebook.com/search/videos/?q=loading"
        page_states = [
            {
                "url": target,
                "ready_state": "loading",
                "title": "",
                "body_text": "",
            },
            {
                "url": target,
                "ready_state": "complete",
                "title": "",
                "body_text": "Please verify you are not a robot before continuing.",
            },
        ]
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6])

        def page_state_fn(_):
            return page_states.pop(0) if page_states else {"url": target, "ready_state": "complete"}

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        ok = fb_keyword_nightly.navigate_with_safety(
            cdp,
            target,
            state=state,
            now_fn=now,
            sleep_fn=lambda _: None,
            page_state_fn=page_state_fn,
            timeout_seconds=2.0,
            poll_interval_seconds=0.1,
        )
        self.assertFalse(ok)
        self.assertEqual(
            state["stop_reason"], fb_keyword_nightly.SAFETY_REASON_CAPTCHA
        )
        self.assertEqual(
            len([m for m in cdp.send_calls if m[0] == PAGE_NAVIGATE_METHOD]),
            1,
        )


class TestNavigationSafetyBudget(unittest.TestCase):
    """Navigation/runtime budget guard tests."""

    def test_navigation_budget(self):
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=60,
            max_navigations=2,
            now_ts=0,
        )
        self.assertIsNone(fb_keyword_nightly._check_navigation_limits(state, 1.0))

        state["navigation_count"] = 2
        self.assertEqual(
            fb_keyword_nightly._check_navigation_limits(state, 1.0),
            fb_keyword_nightly.SAFETY_REASON_NAVIGATION_LIMIT,
        )

    def test_runtime_budget(self):
        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=99,
            now_ts=100.0,
        )
        self.assertIsNone(fb_keyword_nightly._check_navigation_limits(state, 109.9))
        self.assertEqual(
            fb_keyword_nightly._check_navigation_limits(state, 110.1),
            fb_keyword_nightly.SAFETY_REASON_RUNTIME_LIMIT,
        )


class TestPauseSelection(unittest.TestCase):
    """Configurable delay helper tests."""

    def test_inter_reel_delay_bounds(self):
        rng = random.Random(42)
        for _ in range(10):
            pause = fb_keyword_nightly.inter_reel_pause_seconds(rng)
            self.assertGreaterEqual(pause, fb_keyword_nightly.DEFAULT_MIN_INTER_REEL_PAUSE_SECONDS)
            self.assertLessEqual(pause, fb_keyword_nightly.DEFAULT_MAX_INTER_REEL_PAUSE_SECONDS)

    def test_source_switch_delay_bounds(self):
        rng = random.Random(99)
        for _ in range(10):
            pause = fb_keyword_nightly.source_switch_pause_seconds(rng)
            self.assertGreaterEqual(pause, fb_keyword_nightly.DEFAULT_MIN_SOURCE_SWITCH_PAUSE_SECONDS)
            self.assertLessEqual(pause, fb_keyword_nightly.DEFAULT_MAX_SOURCE_SWITCH_PAUSE_SECONDS)


class TestFacebookTargetValidation(unittest.TestCase):
    """Target host + canonicalization checks."""

    def test_canonicalize_source_url_accepts_facebook_alias_hosts(self):
        self.assertEqual(
            fb_keyword_nightly.canonicalize_source_url("https://www.facebook.com/watch/?v=123"),
            "https://www.facebook.com/watch?v=123",
        )
        self.assertEqual(
            fb_keyword_nightly.canonicalize_source_url("https://m.facebook.com/watch/?v=123"),
            "https://m.facebook.com/watch?v=123",
        )

    def test_canonicalize_source_url_rejects_deceptive_host(self):
        self.assertEqual(
            fb_keyword_nightly.canonicalize_source_url("https://facebook.com.evil/watch/?v=123"),
            "",
        )
        self.assertFalse(fb_keyword_nightly.is_direct_reel_video_target("https://facebook.com.evil/watch/?v=123"))

    def test_run_target_urls_drops_non_facebook_targets_without_navigation(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            with patch.object(fb_keyword_nightly, "ensure_browser") as ensure_browser:
                summary = fb_keyword_nightly.run_target_urls(
                    target_urls=[
                        "https://facebook.com.evil/reel/111",
                        "https://evil.example.com/video/222",
                        "not a url",
                    ],
                    out_dir=out_dir,
                )

        self.assertEqual(summary["total_candidates"], 0)
        self.assertEqual(summary["errors"], ["No valid target URLs supplied"])
        ensure_browser.assert_not_called()


class TestSafetyReporting(unittest.TestCase):
    """Safety serialization and report rendering tests."""

    def test_save_latest_tracks_safety_fields(self):
        summary = {
            "started_at": "2026-07-12T00:00:00",
            "query": "safety-test",
            "dry_run": False,
            "safety_stop": True,
            "safety_stop_reason": fb_keyword_nightly.SAFETY_REASON_CAPTCHA,
            "safety_stop_at": "2026-07-12T00:00:00",
            "candidate_count": 1,
            "scraped_count": 0,
            "total_comments": 0,
            "discovered_count": 0,
            "new_count": 0,
            "revisited_count": 0,
            "skipped_current": 0,
            "source_count": 0,
            "explicit_count": 0,
            "selected_count": 0,
            "selected_by_reason": {},
            "output_dir": "/tmp",
            "errors": ["Safety stop: captcha"],
            "runtime_seconds": 12,
            "navigation_count": 5,
            "navigation_limit": 10,
            "runtime_limit_seconds": 20,
        }

        with tempfile.TemporaryDirectory() as td:
            latest_path = Path(td) / "fb_keyword_latest.json"
            with patch.object(fb_keyword_nightly, "STATE_LATEST", latest_path):
                fb_keyword_nightly.save_latest(summary)

            data = json.loads(latest_path.read_text())

        self.assertTrue(data["safety_stop"])
        self.assertEqual(data["safety_stop_reason"], fb_keyword_nightly.SAFETY_REASON_CAPTCHA)
        self.assertEqual(data["runtime_seconds"], 12)
        self.assertEqual(data["navigation_count"], 5)

    def test_save_latest_persists_navigation_verification_without_raw_fields(self):
        cdp = FakeCDP()
        now = _fake_now([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
        raw_secret_title = "Secret title that should not persist"
        raw_secret_body = "Secret body that should not persist"
        raw_secret_token = "oauth-token-secret-value"
        raw_secret_cookie = "session=super-secret-cookie"

        page_states = [
            {
                "url": "https://www.facebook.com/search/videos/?q=private-token-value&x=secret&oauth_token=oauth-token-secret-value",
                "ready_state": "loading",
                "title": raw_secret_title,
                "body_text": raw_secret_body,
                "cookies": {"session": raw_secret_cookie},
                "tokens": {"api": raw_secret_token},
            },
            {
                "url": "https://www.facebook.com/search/videos/?q=private-token-value&x=secret&oauth_token=oauth-token-secret-value",
                "ready_state": "loading",
                "title": "Another secret title",
                "body_text": "Another secret body",
                "cookies": {"session": raw_secret_cookie},
                "tokens": {"api": raw_secret_token},
            },
            {
                "url": "https://www.facebook.com/search/videos/?q=private-token-value&x=secret&oauth_token=oauth-token-secret-value",
                "ready_state": "loading",
                "title": "Another secret title",
                "body_text": "Another secret body",
                "cookies": {"session": raw_secret_cookie},
                "tokens": {"api": raw_secret_token},
            },
        ]

        def page_state_fn(_):
            return (
                page_states.pop(0)
                if page_states
                else {
                    "url": "https://www.facebook.com/search/videos/?q=private-token-value&x=secret&oauth_token=oauth-token-secret-value",
                    "ready_state": "loading",
                }
            )

        state = fb_keyword_nightly.new_fb_safety_state(
            max_runtime_seconds=10,
            max_navigations=5,
            now_ts=0.0,
        )
        with patch.object(fb_keyword_nightly, "MAX_NAVIGATION_VERIFY_SAMPLES", 2):
            ok = fb_keyword_nightly.navigate_with_safety(
                cdp,
                "https://www.facebook.com/search/videos/?q=private-token-value&x=secret",
                state=state,
                now_fn=now,
                sleep_fn=lambda _: None,
                page_state_fn=page_state_fn,
                timeout_seconds=1.4,
                poll_interval_seconds=0.2,
            )
        self.assertFalse(ok)
        state["navigation_verification"]["max_samples"] = (
            fb_keyword_nightly.MAX_NAVIGATION_VERIFY_SAMPLES + 10
        )
        summary = {
            "started_at": "2026-07-12T00:00:00",
            "query": "safe-latest",
            "dry_run": False,
            "candidates": [],
            "scraped": [],
            "tasks": [],
            "total_candidates": 0,
            "total_scraped": 0,
            "total_comments": 0,
            "errors": [],
            "output_dir": "/tmp",
            "target_urls": [],
            "selected_by_reason": {
                "explicit": 0,
                "latest": 0,
                "backfill": 0,
                "revisit": 0,
            },
            "discovered_count": 0,
            "new_count": 0,
            "revisited_count": 0,
            "skipped_current": 0,
            "selected_count": 0,
            "explicit_count": 0,
            "source_count": 0,
            "safety_stop": False,
            "safety_stop_reason": "",
            "safety_stop_at": "",
            "runtime_seconds": 0,
            "navigation_count": 0,
            "navigation_limit": 0,
            "runtime_limit_seconds": 0,
        }
        fb_keyword_nightly.attach_safety_summary(summary, state)

        with tempfile.TemporaryDirectory() as td:
            latest_path = Path(td) / "fb_keyword_latest.json"
            with patch.object(fb_keyword_nightly, "STATE_LATEST", latest_path):
                fb_keyword_nightly.save_latest(summary)
            data = json.loads(latest_path.read_text())

        evidence = data["navigation_verification"]
        self.assertLessEqual(len(evidence["samples"]), 2)
        self.assertEqual(
            evidence["max_samples"],
            fb_keyword_nightly.MAX_NAVIGATION_VERIFY_SAMPLES,
        )
        self.assertTrue(evidence.get("final"))
        rendered = json.dumps(evidence)
        self.assertNotIn(raw_secret_title, rendered)
        self.assertNotIn(raw_secret_body, rendered)
        self.assertNotIn("Another secret title", rendered)
        self.assertNotIn("Another secret body", rendered)
        self.assertNotIn(raw_secret_token, rendered)
        self.assertNotIn(raw_secret_cookie, rendered)
        self.assertNotIn("private-token-value", rendered)
        self.assertNotIn("q=private-token-value", rendered)
        self.assertNotIn("x=secret", rendered)
        self.assertNotIn("oauth_token=oauth-token-secret-value", rendered)
        final_sample = evidence["samples"][-1]
        self.assertIn("query_keys", final_sample["url"])
        final_query_keys = set(final_sample["url"]["query_keys"])
        self.assertIn("q", final_query_keys)
        self.assertIn("x", final_query_keys)
        self.assertNotIn("body_text", final_sample["url"])
        self.assertNotIn("title", final_sample)
        self.assertNotIn("cookies", final_sample)
        self.assertNotIn("tokens", final_sample)

    def test_email_body_mentions_safety_stop(self):
        latest = {
            "output_dir": None,
            "query": "safety-report",
            "candidate_count": 0,
            "scraped_count": 0,
            "total_comments": 0,
            "safety_stop": True,
            "safety_stop_reason": fb_keyword_nightly.SAFETY_REASON_CHECKPOINT,
            "runtime_seconds": 30,
            "runtime_limit_seconds": 60,
            "navigation_count": 4,
            "navigation_limit": 20,
            "errors": ["Safety stop"],
        }
        body = fb_reel_night_cron.build_email_body(
            latest=latest,
            returncode=0,
            stdout="",
            stderr="",
            ingest_result=None,
        )

        self.assertIn("Safety stop: yes", body)
        self.assertIn(fb_keyword_nightly.SAFETY_REASON_CHECKPOINT, body)


class TestReelCronWrapper(unittest.TestCase):
    """Wrapper behavior tests for fb_reel_night_cron."""

    REPO_PYTHON = "/usr/bin/python3"

    def test_wrapper_rejects_unchanged_stale_latest_state(self):
        run_completed = subprocess.CompletedProcess(
            args=["python", "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
        )
        snapshot = {"exists": True, "mtime_ns": 1, "size": 1, "sha256": "same"}

        with patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[snapshot, snapshot]), \
            patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
            patch.object(fb_reel_night_cron, "run", return_value=run_completed), \
            patch.object(fb_reel_night_cron, "ensure_browser"), \
            patch.object(fb_reel_night_cron, "run_keyword_ingest") as ingest, \
            patch.object(fb_reel_night_cron, "send_report") as report:
            exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)
        self.assertEqual(ingest.call_count, 0)
        self.assertTrue(report.called)
        self.assertEqual(
            report.call_args.args[0]["errors"][0],
            "current-run validation failed: latest state did not change for this invocation.",
        )

    def test_wrapper_rejects_mismatched_invocation_or_output_directory(self):
        invocation_id = "nightly-2026-001"
        output_dir = "/tmp/foreign-run"
        completed = subprocess.CompletedProcess(
            args=["python", "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
        )

        with tempfile.TemporaryDirectory() as td:
            latest_path = Path(td) / "fb_keyword_latest.json"
            latest_path.write_text(
                json.dumps({
                    "run_invocation_id": "other-invocation",
                    "output_dir": output_dir,
                    "last_run": datetime.now().isoformat(),
                    "errors": [],
                })
                )
            pre = {"exists": False}
            post = {"exists": True, "mtime_ns": 2, "size": latest_path.stat().st_size, "sha256": "x"}

            with patch.object(fb_reel_night_cron, "LATEST", latest_path), \
                patch.object(fb_reel_night_cron, "current_run_id", return_value=invocation_id), \
                patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[pre, post]), \
                patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
                patch.object(fb_reel_night_cron, "run", return_value=completed), \
                patch.object(fb_reel_night_cron, "ensure_browser"), \
                patch.object(fb_reel_night_cron, "run_keyword_ingest") as ingest, \
                patch.object(fb_reel_night_cron, "send_report") as report:
                exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)
        self.assertEqual(ingest.call_count, 0)
        self.assertIn(
            "does not match this invocation",
            report.call_args.args[0]["errors"][0],
        )

    def test_wrapper_accepts_fresh_correlated_latest_state(self):
        invocation_id = "nightly-2026-002"
        with tempfile.TemporaryDirectory() as td:
            latest_dir = Path(td) / "raw" / "facebook_keyword" / invocation_id
            latest_dir.mkdir(parents=True)
            latest_path = Path(td) / "fb_keyword_latest.json"
            latest_payload = {
                "run_invocation_id": invocation_id,
                "output_dir": str(latest_dir),
                "last_run": datetime.now().isoformat(),
                "errors": [],
            }
            latest_path.write_text(json.dumps(latest_payload))
            pre = {"exists": False}
            post = {"exists": True, "mtime_ns": 3, "size": latest_path.stat().st_size, "sha256": "y"}
            run_completed = subprocess.CompletedProcess(
                args=["python", "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
            )
            ingest_completed = subprocess.CompletedProcess(
                args=["python", "ingest_facebook.py"], returncode=0, stdout="", stderr=""
            )

            with patch.object(fb_reel_night_cron, "LATEST", latest_path), \
                patch.object(fb_reel_night_cron, "current_run_id", return_value=invocation_id), \
                patch.object(fb_reel_night_cron, "_expected_output_dir_for_invocation", return_value=str(latest_dir)), \
                patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[pre, post]), \
                patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
                patch.object(fb_reel_night_cron, "run", return_value=run_completed), \
                patch.object(fb_reel_night_cron, "ensure_browser"), \
                patch.object(fb_reel_night_cron, "run_keyword_ingest", return_value=ingest_completed) as ingest, \
                patch.object(fb_reel_night_cron, "send_report") as report:
                exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 0)
        ingest.assert_called_once_with(str(latest_dir), self.REPO_PYTHON)
        self.assertEqual(report.call_args.args[0]["errors"], [])

    def test_wrapper_does_not_ingest_when_current_run_state_is_invalid(self):
        snapshot = {"exists": True, "mtime_ns": 1, "size": 1, "sha256": "same"}
        run_completed = subprocess.CompletedProcess(
            args=["python", "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
        )

        with patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[snapshot, snapshot]), \
            patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
            patch.object(fb_reel_night_cron, "run", return_value=run_completed), \
            patch.object(fb_reel_night_cron, "ensure_browser"), \
            patch.object(fb_reel_night_cron, "run_keyword_ingest") as ingest, \
            patch.object(fb_reel_night_cron, "send_report"):
            exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)
        ingest.assert_not_called()

    def test_wrapper_outer_inner_failure_yields_nonzero_exit(self):
        invocation_id = "nightly-2026-003"
        with tempfile.TemporaryDirectory() as td:
            latest_dir = Path(td) / f"output-{invocation_id}"
            latest_dir.mkdir()
            latest_path = Path(td) / "fb_keyword_latest.json"
            latest_payload = {
                "run_invocation_id": invocation_id,
                "output_dir": str(latest_dir),
                "last_run": datetime.now().isoformat(),
                "errors": [],
            }
            latest_path.write_text(json.dumps(latest_payload))
            pre = {"exists": False}
            post = {"exists": True, "mtime_ns": 4, "size": latest_path.stat().st_size, "sha256": "z"}
            failed_run = subprocess.CompletedProcess(
                args=["python", "fb_keyword_nightly.py"], returncode=1, stdout="", stderr="boom"
            )
            ingest_completed = subprocess.CompletedProcess(
                args=["python", "ingest_facebook.py"], returncode=0, stdout="", stderr=""
            )

            with patch.object(fb_reel_night_cron, "LATEST", latest_path), \
                patch.object(fb_reel_night_cron, "current_run_id", return_value=invocation_id), \
                patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[pre, post]), \
                patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
                patch.object(fb_reel_night_cron, "run", return_value=failed_run), \
                patch.object(fb_reel_night_cron, "ensure_browser"), \
                patch.object(fb_reel_night_cron, "run_keyword_ingest", return_value=ingest_completed), \
                patch.object(fb_reel_night_cron, "send_report"):
                exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)

    def test_invalid_canonicalization_targets_yield_zero_candidates_and_no_navigation(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            with patch.object(fb_keyword_nightly, "ensure_browser") as ensure_browser:
                summary = fb_keyword_nightly.run_target_urls(
                    target_urls=[
                        "https://www.facebook.com/groups/somegroup",
                        "https://facebook.com.evil/watch/123",
                        "invalid target",
                    ],
                    out_dir=out_dir,
                )

        self.assertEqual(summary["total_candidates"], 0)
        self.assertEqual(summary["errors"], ["No valid target URLs supplied"])
        ensure_browser.assert_not_called()

    def test_wrapper_rejects_output_dir_substring_collision(self):
        invocation_id = "nightly-2026-005"
        with tempfile.TemporaryDirectory() as td:
            expected_output_dir = Path(td) / "raw" / "facebook_keyword" / invocation_id
            expected_output_dir.mkdir(parents=True)
            collision_output_dir = Path(td) / f"{invocation_id}-sibling"
            latest_path = Path(td) / "fb_keyword_latest.json"
            latest_payload = {
                "run_invocation_id": invocation_id,
                "output_dir": str(collision_output_dir),
                "last_run": datetime.now().isoformat(),
                "errors": [],
            }
            latest_path.write_text(json.dumps(latest_payload))
            pre = {"exists": False}
            post = {"exists": True, "mtime_ns": 4, "size": latest_path.stat().st_size, "sha256": "z"}
            run_completed = subprocess.CompletedProcess(
                args=["python", "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
            )

            with patch.object(fb_reel_night_cron, "LATEST", latest_path), \
                patch.object(fb_reel_night_cron, "current_run_id", return_value=invocation_id), \
                patch.object(fb_reel_night_cron, "_expected_output_dir_for_invocation", return_value=str(expected_output_dir)), \
                patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[pre, post]), \
                patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
                patch.object(fb_reel_night_cron, "run", return_value=run_completed), \
                patch.object(fb_reel_night_cron, "ensure_browser"), \
                patch.object(fb_reel_night_cron, "run_keyword_ingest") as ingest, \
                patch.object(fb_reel_night_cron, "send_report") as report:
                exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)
        self.assertEqual(ingest.call_count, 0)
        self.assertIn(
            "output_dir does not match this invocation directory contract",
            report.call_args.args[0]["errors"][0],
        )

    def test_wrapper_rejects_missing_repo_python_before_scrape(self):
        with patch.object(fb_reel_night_cron, "select_repo_python", return_value=None), \
            patch.object(fb_reel_night_cron, "run") as run, \
            patch.object(fb_reel_night_cron, "ensure_browser") as ensure_browser:
            exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 1)
        run.assert_not_called()
        ensure_browser.assert_not_called()

    def test_wrapper_uses_selected_python_for_keyword_and_ingest(self):
        invocation_id = "nightly-2026-004"
        with tempfile.TemporaryDirectory() as td:
            latest_dir = Path(td) / "raw" / "facebook_keyword" / invocation_id
            latest_dir.mkdir(parents=True)
            latest_path = Path(td) / "fb_keyword_latest.json"
            latest_payload = {
                "run_invocation_id": invocation_id,
                "output_dir": str(latest_dir),
                "last_run": datetime.now().isoformat(),
                "errors": [],
            }
            latest_path.write_text(json.dumps(latest_payload))
            pre = {"exists": False}
            post = {"exists": True, "mtime_ns": 4, "size": latest_path.stat().st_size, "sha256": "z"}
            run_completed = subprocess.CompletedProcess(
                args=[self.REPO_PYTHON, "fb_keyword_nightly.py"], returncode=0, stdout="", stderr=""
            )
            ingest_completed = subprocess.CompletedProcess(
                args=[self.REPO_PYTHON, "ingest_facebook.py"], returncode=0, stdout="", stderr=""
            )
            with patch.object(fb_reel_night_cron, "LATEST", latest_path), \
                patch.object(fb_reel_night_cron, "current_run_id", return_value=invocation_id), \
                patch.object(fb_reel_night_cron, "_expected_output_dir_for_invocation", return_value=str(latest_dir)), \
                patch.object(fb_reel_night_cron, "latest_state_snapshot", side_effect=[pre, post]), \
                patch.object(fb_reel_night_cron, "select_repo_python", return_value=self.REPO_PYTHON), \
                patch.object(fb_reel_night_cron, "run", return_value=run_completed) as run, \
                patch.object(fb_reel_night_cron, "ensure_browser"), \
                patch.object(fb_reel_night_cron, "run_keyword_ingest", return_value=ingest_completed) as ingest, \
                patch.object(fb_reel_night_cron, "send_report"):
                exit_code = fb_reel_night_cron.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run.call_args.args[0], [self.REPO_PYTHON, str(fb_reel_night_cron.SCRIPT), "--max-candidates", "10", "--max-scrape", "3"])
        ingest.assert_called_once_with(str(latest_dir), self.REPO_PYTHON)


if __name__ == "__main__":
    unittest.main()
