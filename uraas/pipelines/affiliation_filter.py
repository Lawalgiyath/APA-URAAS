import re
import os
import sys
from scrapy.exceptions import DropItem

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from uraas.config.institutions import get_registry
from uraas.utils.staff_validator import StaffValidator


class AffiliationFilterPipeline:
    """
    Multi-institution affiliation filter.
    Only allows papers with at least ONE confirmed staff member from the target institution.
    Prevents false positives from papers that just mention the institution name.
    """
    
    def __init__(self):
        self.registry = get_registry()
        self.validators = {}  # Cache validators per institution
        self.institution_patterns = {}  # Cache compiled patterns per institution
    
    def _ensure_institution(self, spider):
        """Lazily initialize institution config from the spider (called in process_item)."""
        institution_name = getattr(spider, 'institution', 'unilag')
        if getattr(self, '_initialized_for', None) == institution_name:
            return  # Already set up for this institution

        institution_config = self.registry.get(institution_name)
        if not institution_config:
            spider.logger.warning(f"Institution '{institution_name}' not found, using default (UNILAG)")
            institution_config = self.registry.get('unilag')

        self.current_institution = institution_config
        self.current_validator = StaffValidator(institution_config=institution_config)
        self.current_patterns = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in institution_config.affiliation_patterns
        ]
        self._initialized_for = institution_name
        spider.logger.info(f"Affiliation filter initialized for {institution_config.name}")
        spider.logger.info(f"Staff count: {len(self.current_validator.staff_names)}")
        spider.logger.info(f"Affiliation patterns: {len(self.current_patterns)}")

    def is_institution_affiliated(self, text: str) -> bool:
        """Check if unstructured text contains institution references"""
        if not text:
            return False
        return any(pattern.search(text) for pattern in self.current_patterns)

    def process_item(self, item, spider):
        """
        Validate that paper has at least one confirmed staff member.

        Steps:
        1. Check if at least ONE author is a confirmed staff member (MANDATORY)
        2. Optionally verify affiliation text as secondary confirmation
        3. Tag item with institution ROR
        """
        try:
            # Initialize institution config lazily (spider is available here)
            self._ensure_institution(spider)

            authors = item.get('authors', [])

            # Handle empty or invalid authors list
            if not authors or not isinstance(authors, list):
                raise DropItem(
                    f"Paper '{item.get('title', 'Unknown')[:60]}...' has no valid authors list."
                )

            # CRITICAL: Validate against staff list with RELAXED threshold (75%)
            matching_staff = []
            for author in authors:
                try:
                    if self.current_validator.is_staff_member(author, fuzzy_threshold=75):
                        matching_staff.append(author)
                except Exception as e:
                    spider.logger.error(f"Error validating author '{author}': {str(e)}")
                    continue

            if not matching_staff:
                raise DropItem(
                    f"Paper '{item.get('title', 'Unknown')[:60]}...' has NO confirmed "
                    f"{self.current_institution.name} staff authors. "
                    f"Authors: {', '.join(str(a) for a in authors[:3])}"
                )

            # Store which authors are staff for metadata
            item['staff_authors'] = matching_staff
            item['unilag_staff_authors'] = matching_staff  # Legacy field for backward compatibility

            # Tag with institution ROR
            item['institution'] = self.current_institution.name
            item['institution_ror'] = self.current_institution.ror

            # Secondary validation: Check affiliation text (optional but recommended)
            has_affiliation = False

            try:
                # Check explicit affiliations list
                affiliations = item.get('affiliations', [])
                if isinstance(affiliations, list):
                    if any(self.is_institution_affiliated(aff) for aff in affiliations if aff):
                        has_affiliation = True

                # Check raw string summary
                if not has_affiliation:
                    raw_text = ' '.join(str(a) for a in affiliations if a) if isinstance(affiliations, list) else ''
                    raw_text += " " + str(item.get('raw_affiliation', ''))
                    if self.is_institution_affiliated(raw_text):
                        has_affiliation = True

            except Exception as e:
                spider.logger.error(
                    f"Error checking affiliations for paper '{item.get('title', 'Unknown')[:60]}': {str(e)}"
                )

            # Log warning if staff member found but no affiliation text
            if not has_affiliation:
                spider.logger.warning(
                    f"Paper has {self.current_institution.short_name} staff ({item['staff_authors']}) "
                    f"but no affiliation text. Proceeding with caution."
                )

            return item

        except DropItem:
            raise
        except Exception as e:
            spider.logger.error(
                f"Unexpected error in affiliation filter for paper '{item.get('title', 'Unknown')[:60]}': {str(e)}"
            )
            raise DropItem(f"Failed to process item due to error: {str(e)}")
