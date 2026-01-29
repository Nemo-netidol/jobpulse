from include.models import Job
from datetime import datetime

class RemoteOKAdapter:
    @staticmethod
    def transform(raw_job: dict) -> Job:
        return Job(
            title=raw_job.get('position', 'Unknown Title'),
            company=raw_job.get('company') or 'Unknown',
            description=raw_job.get('description', ''),
            url=raw_job.get('url', ''),
            location=raw_job.get('location') or 'Unknown',
            posted_date=raw_job.get('date'),
            scraped_at=datetime.now()
        )