from include.models import Job
from datetime import datetime
from include.JobClassificationService import classify_category

class RemoteOKAdapter:
    @staticmethod
    def transform(raw_job: dict) -> Job:
        if not raw_job.get('position') or not raw_job.get('company'):
            return None

        return Job(
            title=raw_job.get('position', 'Unknown Title'),
            company=raw_job.get('company') or 'Unknown',
            description=raw_job.get('description', ''),
            url=raw_job.get('url') if raw_job.get('url') else None,
            location=raw_job.get('location') or 'Unknown',
            posted_date=raw_job.get('date'),
            scraped_at=datetime.now(),
            category=classify_category(raw_job.get('position'), raw_job.get('description'))
        )