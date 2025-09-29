#!/usr/bin/env python3
"""
Extract unique contacts from one or more Gmail Takeout .mbox files (Sent Mail only)
into hubspot_contacts.csv.

Usage:
    python extract_contacts.py path/to/one.mbox [path/to/two.mbox ...]

Output:
    hubspot_contacts.csv with columns: email, first_name, last_name, phone, last_contacted, notes

Notes:
- Filters messages to Sent Mail using X-Gmail-Labels (contains "Sent" or "Sent Mail" or "Sent Items").
- Deduplicates by email, keeping the latest Date header as last_contacted.
- Extracts phone numbers from the email body via regex (first match per contact).
- Notes are the first 200 characters of the email body of the latest message used for that contact.
- Streams through the mbox to handle large files efficiently.
- When multiple files are provided, contacts and statistics are merged across all inputs.
"""
from __future__ import annotations

import argparse
import csv
import mailbox
import re
from email.utils import getaddresses, parsedate_to_datetime
from typing import Dict, Optional, Tuple


PHONE_REGEX = re.compile(
    r"""
    (?:\+?\d{1,3}[\s.-]?)?               # optional country code
    (?:\(?\d{3}\)?[\s.-]?)              # area code
    \d{3}[\s.-]?\d{4}                    # local number
    |                                       # OR international-ish condensed
    \+\d{7,15}                             # +########### up to 15 digits
    """,
    re.VERBOSE,
)

# Label tokens that imply Sent Mail. Gmail Takeout usually includes X-Gmail-Labels
SENT_LABEL_TOKENS = {"sent", "sent mail", "sent items"}

# Patterns to exclude automated/system emails
AUTOMATED_EMAIL_PATTERNS = [
    r'.*@.*\.hubspotemail\.net$',
    r'.*@.*\.hubspot\.com$',
    r'.*@bcc\.hubspot\.com$',
    r'.*@.*unsubscribe.*',
    r'.*@.*noreply.*',
    r'.*@.*no-reply.*',
    r'.*@.*donotreply.*',
    r'.*@.*bounce.*',
    r'.*@.*mailer-daemon.*',
    r'^[a-f0-9]{20,}@.*',  # Long hex strings (tracking IDs)
    r'.*\+.*=.*@.*',      # Plus addressing with equals (tracking)
    r'^\d+@.*',           # Numeric IDs at start
    r'.*-.*=.*@.*',       # Dash with equals (more tracking)
    r'.*@.*\.linuxfoundation\.org$',  # Automated from your sample
]


def is_sent_message(msg: mailbox.mboxMessage, assume_sent_if_no_labels: bool = False) -> bool:
    """Determine if a message belongs to Sent Mail based on labels.

    We conservatively check X-Gmail-Labels for a token matching SENT_LABEL_TOKENS.
    If the header is absent, we return False (to avoid including non-sent messages).
    """
    labels = msg.get("X-Gmail-Labels") or msg.get("X-Gmail-Labels".lower())
    if not labels:
        return assume_sent_if_no_labels
    label_tokens = {t.strip().lower() for t in labels.split(',')}
    return any(token in label_tokens for token in SENT_LABEL_TOKENS)


def parse_body_text(msg: mailbox.mboxMessage) -> str:
    """Extract a best-effort plain text body from the message.

    Prefers text/plain parts; falls back to decoding the payload directly.
    Returns a unicode string with reasonable whitespace normalization.
    """
    parts: list[Tuple[Optional[str], bytes]] = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or '').lower()
            if content_type.startswith("text/plain"):
                try:
                    payload = part.get_payload(decode=True)
                except Exception:
                    payload = None
                if payload:
                    parts.append((part.get_content_charset() or part.get_charset() or 'utf-8', payload))
    else:
        try:
            payload = msg.get_payload(decode=True)
        except Exception:
            payload = None
        if payload:
            parts.append((msg.get_content_charset() or msg.get_charset() or 'utf-8', payload))

    texts: list[str] = []
    for charset, payload in parts:
        try:
            texts.append(payload.decode(str(charset) if charset else 'utf-8', errors='replace'))
        except Exception:
            try:
                texts.append(payload.decode('utf-8', errors='replace'))
            except Exception:
                continue

    body = "\n".join(texts).strip()
    # Normalize simple excessive whitespace
    body = re.sub(r"\s+", " ", body)
    return body


def is_automated_email(email: str) -> bool:
    """Check if an email looks like an automated/system email."""
    email_lower = email.lower()
    for pattern in AUTOMATED_EMAIL_PATTERNS:
        if re.match(pattern, email_lower):
            return True
    return False


def split_name(name: str) -> Tuple[str, str]:
    """Split a display name into first_name and last_name.

    Strategy: split on whitespace; first token -> first_name; last token -> last_name;
    if only one token, last_name is empty. Strips common surrounding quotes.
    """
    if not name:
        return "", ""
    name = name.strip().strip('"').strip("'")
    if not name:
        return "", ""
    tokens = name.split()
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[0], tokens[-1]


def extract_contacts(mbox_path: str) -> Tuple[Dict[str, Dict[str, str]], Dict[str, int]]:
    """Scan the .mbox and build a dict keyed by email with aggregated contact info.

    Returns a tuple (contacts, stats)
      - contacts: dict email -> {first_name, last_name, phone, last_contacted (ISO), notes}
      - stats:    {
            total_messages,
            sent_messages,
            messages_with_recipients,
            parse_errors,
            unique_emails
        }
    """
    contacts: Dict[str, Dict[str, str]] = {}

    stats = {
        'total_messages': 0,
        'sent_messages': 0,
        'messages_with_recipients': 0,
        'parse_errors': 0,
        'unique_emails': 0,
        'automated_emails_filtered': 0,
    }

    mbox = mailbox.mbox(mbox_path)
    # If the file path implies it's a Sent mailbox (common in Takeout exports),
    # include messages even when X-Gmail-Labels are missing.
    assume_sent = any(tok in mbox_path.lower() for tok in ("/sent", "\\sent", "sent mail", "sent_mail", "-sent", "sent.mbox", "sent"))

    for msg in mbox:
        stats['total_messages'] += 1
        try:
            if not is_sent_message(msg, assume_sent_if_no_labels=assume_sent):
                continue
            stats['sent_messages'] += 1

            # Parse recipients from To and Cc
            to_addrs = msg.get_all('To', [])
            cc_addrs = msg.get_all('Cc', [])
            pairs = getaddresses(to_addrs + cc_addrs)
            if not pairs:
                continue
            stats['messages_with_recipients'] += 1

            # Parse date
            date_hdr = msg.get('Date')
            dt = None
            if date_hdr:
                try:
                    dt = parsedate_to_datetime(date_hdr)
                except Exception:
                    dt = None

            # Extract body once per message
            body = parse_body_text(msg)
            phone_match = PHONE_REGEX.search(body or "")
            phone = phone_match.group(0) if phone_match else ""
            # Notes are first 200 chars of body
            notes = (body or "")[:200]

            iso_date = dt.isoformat() if dt else ""

            for display_name, email in pairs:
                email = (email or '').strip().lower()
                if not email:
                    continue
                if is_automated_email(email):
                    stats['automated_emails_filtered'] += 1
                    continue
                first_name, last_name = split_name(display_name)

                existing = contacts.get(email)
                if existing is None:
                    contacts[email] = {
                        'email': email,
                        'first_name': first_name,
                        'last_name': last_name,
                        'phone': phone,
                        'last_contacted': iso_date,
                        'notes': notes,
                    }
                else:
                    # Keep earliest meaningful names but allow filling missing
                    if not existing['first_name'] and first_name:
                        existing['first_name'] = first_name
                    if not existing['last_name'] and last_name:
                        existing['last_name'] = last_name

                    # Update to latest date for last_contacted and notes
                    def parse_iso(d: str) -> Optional[str]:
                        return d or None

                    replace = False
                    if iso_date and (not existing['last_contacted'] or iso_date > existing['last_contacted']):
                        replace = True
                    if replace:
                        existing['last_contacted'] = iso_date
                        existing['notes'] = notes

                    # Fill phone if empty; else prefer a phone from the message with latest date
                    if phone and (not existing['phone'] or replace):
                        existing['phone'] = phone

        except Exception:
            # Robust to any single-message parse errors; continue streaming
            stats['parse_errors'] += 1
            continue

    stats['unique_emails'] = len(contacts)
    return contacts, stats


def merge_contact(existing: Dict[str, str], incoming: Dict[str, str]) -> None:
    """Merge a single incoming contact record into an existing one in-place.

    Keeps earliest meaningful names but fills missing. Updates last_contacted/notes/phone
    when the incoming has a later contact date.
    """
    # Fill names if missing
    if not existing.get('first_name') and incoming.get('first_name'):
        existing['first_name'] = incoming['first_name']
    if not existing.get('last_name') and incoming.get('last_name'):
        existing['last_name'] = incoming['last_name']

    # Determine if incoming is more recent
    existing_date = existing.get('last_contacted') or ""
    incoming_date = incoming.get('last_contacted') or ""
    replace = bool(incoming_date and (not existing_date or incoming_date > existing_date))
    if replace:
        existing['last_contacted'] = incoming_date
        existing['notes'] = incoming.get('notes', existing.get('notes', ''))

    # Prefer phone from the more recent record, or fill if empty
    incoming_phone = incoming.get('phone') or ""
    if incoming_phone and (replace or not existing.get('phone')):
        existing['phone'] = incoming_phone


def extract_contacts_from_paths(mbox_paths: list[str]) -> Tuple[Dict[str, Dict[str, str]], Dict[str, int]]:
    """Process multiple mbox files, merging contacts and aggregating stats."""
    all_contacts: Dict[str, Dict[str, str]] = {}
    total_stats = {
        'total_messages': 0,
        'sent_messages': 0,
        'messages_with_recipients': 0,
        'parse_errors': 0,
        'unique_emails': 0,  # recomputed below
        'automated_emails_filtered': 0,
    }

    for path in mbox_paths:
        contacts, stats = extract_contacts(path)
        # Merge stats (unique_emails will be recomputed at the end)
        for k in total_stats:
            if k != 'unique_emails':
                total_stats[k] += stats.get(k, 0)
        # Merge contacts
        for email, incoming in contacts.items():
            existing = all_contacts.get(email)
            if existing is None:
                all_contacts[email] = dict(incoming)
            else:
                merge_contact(existing, incoming)

    total_stats['unique_emails'] = len(all_contacts)
    return all_contacts, total_stats


def write_csv(contacts: Dict[str, Dict[str, str]], output_path: str) -> None:
    fieldnames = ['email', 'first_name', 'last_name', 'phone', 'last_contacted', 'notes']
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _, row in sorted(contacts.items(), key=lambda kv: kv[0]):
            writer.writerow({k: row.get(k, '') for k in fieldnames})


def main():
    parser = argparse.ArgumentParser(description='Extract contacts from Gmail Sent Mail .mbox')
    parser.add_argument('mbox_paths', nargs='+', help='Path(s) to Gmail Takeout .mbox file(s) (Sent Mail)')
    parser.add_argument('--output', '-o', default='hubspot_contacts.csv', help='Output CSV path (default: hubspot_contacts.csv)')
    parser.add_argument('--stats', action='store_true', help='Print processing statistics')
    args = parser.parse_args()

    contacts, stats = extract_contacts_from_paths(args.mbox_paths)
    write_csv(contacts, args.output)
    print(f"Wrote {len(contacts)} contacts to {args.output}")
    if args.stats:
        print("\nProcessing stats:")
        print(f"  Total messages scanned:      {stats['total_messages']}")
        print(f"  Messages considered Sent:    {stats['sent_messages']}")
        print(f"  Messages with recipients:    {stats['messages_with_recipients']}")
        print(f"  Unique email addresses:      {stats['unique_emails']}")
        print(f"  Automated emails filtered:   {stats['automated_emails_filtered']}")
        print(f"  Message parse errors:        {stats['parse_errors']}")


if __name__ == '__main__':
    main()
