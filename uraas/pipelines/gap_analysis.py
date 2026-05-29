from scrapy.exceptions import DropItem
from uraas.database import SessionLocal, Item
from uraas.utils.normalizer import normalize_title
from thefuzz import fuzz

FUZZY_THRESHOLD = 95  # % similarity required to classify as duplicate

class GapAnalysisPipeline:
    """
    Phase 2: The Gap Analysis (Fuzzy Edition).
    1. Check DOI first (exact, deterministic).
    2. If no DOI, fuzzy-compare normalized title via Levenshtein distance.
       If similarity >= 95% → drop as duplicate.
    """
    def open_spider(self):
        self.session = SessionLocal()
        # Cache existing normalized titles for fast in-memory fuzzy comparison
        self._cached_titles = [
            normalize_title(r[0])
            for r in self.session.query(Item.dc_title).all()
            if r[0]
        ]

    def close_spider(self):
        try:
            self.session.close()
        except Exception:
            pass

    def process_item(self, item, spider):
        try:
            # --- Step 1: DOI exact match ---
            if item.get('doi'):
                exists = self.session.query(Item).filter_by(doi=item['doi']).first()
                if exists:
                    spider.logger.info(f"[Gap] DOI duplicate: {item['doi']}")
                    raise DropItem(f"Duplicate DOI: {item['doi']}")

            # --- Step 2: URL exact match ---
            if item.get('url'):
                exists = self.session.query(Item).filter_by(url=item['url']).first()
                if exists:
                    spider.logger.info(f"[Gap] URL duplicate: {item['url']}")
                    raise DropItem(f"Duplicate URL: {item['url']}")

            # --- Step 3: Fuzzy title match (only when no DOI for deterministic check) ---
            if not item.get('doi') and item.get('title'):
                needle = normalize_title(item['title'])
                for cached in self._cached_titles:
                    score = fuzz.ratio(needle, cached)
                    if score >= FUZZY_THRESHOLD:
                        spider.logger.info(
                            f"[Gap] Fuzzy duplicate ({score}%): '{item['title'][:60]}'"
                        )
                        raise DropItem(f"Fuzzy duplicate title ({score}%)")

            # Survives all checks → it's a genuine gap, add to cache for this session
            self._cached_titles.append(normalize_title(item.get('title', '')))
            return item
            
        except DropItem:
            raise
        except Exception as e:
            spider.logger.error(f"[Gap] Error processing item: {e}")
            # Don't drop on error, let it through
            return item

