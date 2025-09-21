# Marketing Scripts

Automation scripts to support marketing workflows: contact extraction, data cleanup, and other utilities.

## extract_mbox_contacts.py

Parse a Gmail Takeout `.mbox` file of Sent Mail to produce a deduplicated contact list for import into CRMs like HubSpot.

What it does:
- Extracts recipients from `To` and `Cc` headers of Sent messages only.
- Deduplicates by email address.
- Splits names into `first_name` and `last_name`.
- Tries to extract a phone number from the email body using a regex.
- Uses the email `Date` header as `last_contacted` (keeps the latest per contact).
- Saves a short note (first 200 characters of body) from the latest message per contact.
- Filters out common automated/system email addresses (e.g., HubSpot trackers, no-reply, unsubscribe links).

Output CSV columns:
- `email`
- `first_name`
- `last_name`
- `phone`
- `last_contacted` (ISO timestamp)
- `notes` (first 200 chars of body)

### Usage

```bash
python extract_mbox_contacts.py path/to/Sent-001.mbox
```

Options:
- `-o, --output`: Output CSV path (default: `hubspot_contacts.csv`).
- `--stats`: Print processing statistics (total messages, sent messages, messages with recipients, unique emails, automated emails filtered, parse errors).

Notes:
- Sent detection primarily uses the `X-Gmail-Labels` header. If missing, the script assumes Sent if the file path suggests a Sent mailbox (e.g., contains `sent`, `Sent-001.mbox`).
- The script streams the mbox using `mailbox.mbox` and is resilient to individual message parse errors.
- Only the Python standard library is required.

### Examples

```bash
# Basic usage
python extract_mbox_contacts.py "C:\\Users\\me\\Downloads\\Sent-001.mbox"

# Custom output with stats
python extract_mbox_contacts.py "C:\\Users\\me\\Downloads\\Sent-001.mbox" -o contacts.csv --stats
```

### Development

- CSV files are ignored by git via `.gitignore`.
- Tested on Windows with Python 3.11+.
