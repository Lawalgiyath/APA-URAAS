# Define your item pipelines here
from uraas.database import SessionLocal, Item, Author, Collection, Community, File
from uraas.utils.unilag_classifier import classifier
from uraas.utils.pdf_downloader import pdf_downloader
from uraas.utils.ai_classifier import extract_keywords, _clean_text, classify_special_collections
from uraas.utils.analytics_cache import analytics_cache
from datetime import date
import re

_DOI_RE = re.compile(r'^10\.\d{4,}/')


def _validate_doi(doi: str) -> bool:
    """Returns True if the DOI has a valid format."""
    if not doi:
        return False
    # Clean common prefixes
    doi = doi.replace('https://doi.org/', '').replace('http://dx.doi.org/', '').strip()
    return bool(_DOI_RE.match(doi))


class DatabaseStoragePipeline:
    def open_spider(self):
        self.session = SessionLocal()
        self._cache_invalidated = False

    def close_spider(self):
        try:
            self.session.close()
        except Exception:
            pass
        # Invalidate analytics cache so fresh data shows immediately
        if self._cache_invalidated:
            analytics_cache.invalidate_all()

    def process_item(self, item, spider):
        try:
            # Validate item has required fields
            if not item.get('title'):
                spider.logger.error("Item missing title, skipping")
                return item

            doi = item.get('doi') or None

            # Validate DOI format — reject malformed ones
            if doi and not _validate_doi(doi):
                spider.logger.warning(f"Malformed DOI rejected: {doi!r}")
                doi = None

            # Deduplicate by DOI first, then by URL, then by title
            if doi:
                doi = doi.replace('https://doi.org/', '').replace('http://dx.doi.org/', '').strip()
                existing = self.session.query(Item).filter_by(doi=doi).first()
                if existing:
                    spider.logger.debug(f"Duplicate DOI skipped: {doi}")
                    return item

            url = item.get('url')
            if url:
                existing = self.session.query(Item).filter_by(url=url).first()
                if existing:
                    spider.logger.debug(f"Duplicate URL skipped: {url}")
                    return item

            # Deduplicate by normalised title (avoid same title from multiple sources)
            norm_title = (item.get('title') or '').strip().lower()[:200]
            if norm_title:
                existing = self.session.query(Item).filter(
                    Item.title.ilike(norm_title[:100])
                ).first()
                if existing:
                    spider.logger.debug(f"Duplicate title skipped: {norm_title[:60]}")
                    return item

            # Classify the document using enhanced classifier
            try:
                text_corpus = f"{item.get('title', '')} {item.get('abstract', '')} {item.get('raw_affiliation', '')}"
                classifications = classifier.classify(text_corpus, threshold=0.5)
            except Exception as e:
                spider.logger.error(f"Classification error: {e}")
                classifications = []

            provenance = f"Harvested via URAAS Crawler - {date.today().isoformat()}"

            # Extract AI keywords from title+abstract using the proper classifier
            try:
                ai_kws = extract_keywords(
                    item.get('title', ''),
                    item.get('abstract', ''),
                    top_n=20
                )
                tags = [k['word'] for k in ai_kws]
            except Exception as e:
                spider.logger.error(f"Keyword extraction error: {e}")
                tags = []

            # Determine institution from spider context
            institution_name = getattr(spider, 'institution_name', None)
            institution_ror  = getattr(spider, 'ror_id', None)

            # Special Collections scoring — heavy weight on indigenous knowledge,
            # cultural heritage, African literature, etc. Score>0 marks the item as
            # part of a special collection; drives ranking on the dashboard.
            sc_score = 0.0
            sc_categories = ''
            try:
                sc_hits = classify_special_collections(
                    item.get('title', ''),
                    item.get('abstract', ''),
                    item.get('dc_subject', '') or ', '.join(tags[:15]),
                )
                if sc_hits:
                    sc_score = float(sum(h['score'] for h in sc_hits))
                    sc_categories = ','.join(h['category'] for h in sc_hits)
                    spider.logger.info(
                        f"SC HIT (score={sc_score:.1f}, cats={sc_categories}): "
                        f"{(item.get('title') or '')[:80]}"
                    )
            except Exception as e:
                spider.logger.error(f"Special-collections scoring error: {e}")

            # Create Item with Dublin Core metadata
            doc = Item(
                title=item.get('title'),
                dc_title=item.get('title'),
                dc_identifier_uri=doi or item.get('url'),
                dc_identifier_doi=doi,
                dc_description_provenance=provenance,
                dc_rights=item.get('dc_rights', 'info:eu-repo/semantics/restrictedAccess'),
                abstract=item.get('abstract') or None,
                doi=doi,
                url=item.get('url') or 'https://openalex.org',
                source_repository=item.get('source_repository'),
                pdf_url=item.get('pdf_url'),
                # AI keywords (comma-separated)
                dc_subject=', '.join(tags[:15]),
                ai_keywords=', '.join(tags),
                sdg_tags=item.get('sdg_tags'),
                # Institution tracking for multi-institution analytics
                institution=institution_name,
                ror=institution_ror,
                # Special Collections weighting
                special_collection_score=sc_score,
                special_collection_categories=sc_categories,
            )

            # Log to stdout for dashboard terminal
            try:
                safe_title = (item.get('title') or '').encode('ascii', errors='replace').decode('ascii')
                print(f"URAAS_DOWNLOAD: {safe_title}", flush=True)
            except Exception:
                print(f"URAAS_DOWNLOAD: [Title encoding error]", flush=True)

            # Create Authors
            authors_full = item.get('authors_full', [])
            if not authors_full:
                # Fallback to simple list if authors_full is missing
                for a in item.get('authors', []):
                    authors_full.append({'name': a, 'orcid': '', 'ror': ''})

            for auth in authors_full:
                author_name = auth.get('name', '')
                try:
                    if not author_name or not isinstance(author_name, str):
                        continue
                    author_obj = self.session.query(Author).filter_by(
                        normalized_name=author_name.lower().strip()
                    ).first()
                    
                    if not author_obj:
                        author_obj = Author(
                            name=author_name,
                            normalized_name=author_name.lower().strip(),
                            orcid=auth.get('orcid', ''),
                            ror=auth.get('ror', '')
                        )
                        self.session.add(author_obj)
                    else:
                        # Update missing IDs if they are newly discovered
                        if auth.get('orcid') and not author_obj.orcid:
                            author_obj.orcid = auth['orcid']
                        if auth.get('ror') and not author_obj.ror:
                            author_obj.ror = auth['ror']

                    doc.authors.append(author_obj)
                except Exception as e:
                    spider.logger.error(f"Error processing author '{author_name}': {e}")
                    continue

            self.session.add(doc)
            self.session.flush()  # Get doc.id

            # Map classified collections
            try:
                for community_name, collection_name, score in classifications[:3]:
                    try:
                        coll_obj = self.session.query(Collection).filter_by(name=collection_name).first()
                        if coll_obj and coll_obj not in doc.collections:
                            doc.collections.append(coll_obj)
                    except Exception as e:
                        spider.logger.error(f"Error adding collection '{collection_name}': {e}")
                        continue
            except Exception as e:
                spider.logger.error(f"Error processing classifications: {e}")

            # Download PDF if available
            if doc.pdf_url:
                try:
                    policy = item.get('suggested_access', 'Private')
                    pdf_metadata = pdf_downloader.download_pdf(doc.pdf_url, doc.id)
                    if pdf_metadata:
                        bitstream = File(
                            item_id=doc.id,
                            file_path=pdf_metadata['file_path'],
                            sha256_hash=pdf_metadata['sha256_hash'],
                            access_policy=policy
                        )
                        self.session.add(bitstream)
                except Exception as e:
                    spider.logger.error(f"PDF download error: {e}")

            self.session.commit()
            self._cache_invalidated = True
            return item

        except Exception as e:
            spider.logger.error(f"Database storage error for '{item.get('title', 'Unknown')[:60]}': {e}")
            try:
                self.session.rollback()
            except Exception:
                pass
            raise
