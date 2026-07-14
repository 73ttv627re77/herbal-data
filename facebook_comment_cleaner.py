#!/usr/bin/env python3
"""Deterministic Facebook comment body cleaning utilities."""

import re
from typing import Optional


def _normalize_author_hint(raw_author: Optional[str]) -> str:
    """Normalize an author hint by dropping appended UI time suffixes."""
    if not raw_author:
        return ""

    author = str(raw_author).strip()
    if not author:
        return ""

    author = re.sub(
        r"\s+\d+\s*(?:week|weeks|day|days|hour|hours|minute|minutes|month|months|year|years)\s*ago\s*$",
        "",
        author,
        flags=re.IGNORECASE,
    )
    author = re.sub(r"\s+a\s+week\s+ago\s*$", "", author, flags=re.IGNORECASE)
    author = re.sub(r"\s+\d+[smhdwy]\s*$", "", author, flags=re.IGNORECASE)
    return " ".join(author.strip().split())


def _collapse_repeated_word_prefix(words: list[str], min_len: int = 2) -> list[str]:
    """Collapse duplicated exact leading phrases (`w ... w` -> one copy)."""
    if len(words) < min_len * 2:
        return words

    changed = True
    while changed:
        changed = False
        max_len = min(len(words) // 2, 80)
        for phrase_len in range(max_len, min_len - 1, -1):
            if len(words) < phrase_len * 2:
                continue

            block = words[:phrase_len]
            repeats = 1
            while len(words) >= (repeats + 1) * phrase_len and (
                words[repeats * phrase_len : (repeats + 1) * phrase_len] == block
            ):
                repeats += 1

            if repeats > 1:
                words = words[(repeats - 1) * phrase_len :]
                changed = True
                break
    return words


def clean_facebook_comment_body(
    raw_text: Optional[str],
    author_hint: Optional[str] = None,
) -> str:
    """Return a cleaned version of a Facebook comment body."""
    if raw_text is None:
        return ""

    text = str(raw_text).replace("\u00a0", " ").strip()
    if not text:
        return ""

    # Remove badge/metadata fragments that sometimes appear at the start.
    text = re.sub(
        r"(?i)^(?:top fan(?:\s+top fan)*\s+)?view information about identity badges\s+",
        "",
        text,
    )
    text = re.sub(r"(?i)^view information about identity badges\s+", "", text)
    text = re.sub(r"(?i)^top fan(?:\s+top fan)*\s*", "", text)

    # Normalize duplicate author prefixes if an author hint exists.
    author = _normalize_author_hint(author_hint)
    if author:
        author_re = re.escape(author)
        text = re.sub(
            rf"^(?:{author_re}\s+){{2,}}",
            f"{author} ",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"^(?:{author_re}\s+){{1,}}{author_re}\b",
            f"{author} ",
            text,
            flags=re.IGNORECASE,
        )

    # Remove common Facebook chrome/control text and reaction artifacts.
    text = text.replace("…", " ... ")
    text = re.sub(r"(?i)\bsee more\b(?:\s+\bsee more\b)*", " ", text)
    text = re.sub(r"(?i)\bhide or report this\b", " ", text)
    text = re.sub(r"(?i)\bedited\b(?:\s+\bedited\b)+", " ", text)
    text = re.sub(r"(?i)\blike\b(?:\s+\blike\b)+", " ", text)
    text = re.sub(r"(?i)\breact\b(?:\s+\breact\b)+", " ", text)
    text = re.sub(r"(?i)\breply\b(?:\s+\breply\b)+", " ", text)
    text = re.sub(r"(?i)\b\d+\s*reactions?\b", " ", text)
    text = re.sub(r"(?i)\bsee who reacted to this\b", " ", text)
    text = re.sub(r"\b\d+[smhdwy]\b(?:\s+\b\d+[smhdwy]\b)+", " ", text)

    # Remove any lone control words that slipped through in a single copy.
    text = re.sub(r"(?i)\b(like|react|reply|edited|hide|report)\b", " ", text)

    words = [w for w in re.sub(r"\s+", " ", text).strip().split(" ") if w]

    # Repeated accessibility-tree payloads usually include the leading author
    # once, then duplicate the actual body. Collapse duplicated body content after
    # an optional author prefix.
    author_words = []
    if author:
        author_words = [w for w in author.split() if w]
        if words[:len(author_words)] == author_words:
            collapsed_tail = _collapse_repeated_word_prefix(words[len(author_words):])
            if collapsed_tail != words[len(author_words):]:
                words = author_words + collapsed_tail

    words = _collapse_repeated_word_prefix(words)

    # Remove duplicated numeric reaction counters that duplicate from accessibility trees.
    cleaned_words: list[str] = []
    previous = None
    for word in words:
        if previous is not None and word == previous and word.isdigit():
            continue
        cleaned_words.append(word)
        previous = word
    text = " ".join(cleaned_words)

    # Trim leftovers.
    text = text.replace(";", " ")
    text = re.sub(r"\.\.\.\s*\.\.\.", "...", text)
    text = re.sub(r"\b\d+\s*$", "", text)
    text = re.sub(r"\s+\.\.\.\s*$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" \"“”‘’")
    return text.strip()
