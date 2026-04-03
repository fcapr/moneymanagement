# moneymanagement

Flask app for personal finance tracking.

## Safer Setup

Use environment variables for secrets and database connection:

- `SECRET_KEY`: required for secure session signing
- `DATABASE_URL`: PostgreSQL connection string
- `FLASK_ENV`: set to `production` in production

The app now:

- Rejects POST/PUT/PATCH/DELETE without a valid CSRF token
- Applies baseline security headers (CSP, frame blocking, content-type sniffing protection)
- Uses stricter server-side validation for transaction fields
- Avoids exposing raw exception text to users

## Local Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export SECRET_KEY='replace-with-long-random-secret'
export DATABASE_URL='postgresql://user:password@localhost:5432/finanze'
python app.py
```

## Local Tests

```bash
python -m unittest discover -s tests -v
```
