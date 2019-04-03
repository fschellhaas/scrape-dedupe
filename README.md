```bash
# install dependencies
pip install -r requirements.txt

# scrape descendant citations off google scholar
python scrape_google_scholar.py

# deduplicate records
python find_dupes.py
```