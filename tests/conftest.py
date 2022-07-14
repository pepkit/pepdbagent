import os
from webbrowser import get
import pytest
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture
def database_credentials(request):
    return {
        'POSTGRES_USER': os.environ.get('POSTGRES_USER') or "postgres",
        'POSTGRES_PASSWORD': os.environ.get('POSTGRES_PASSWORD') or "docker",
        'POSTGRES_DB': os.environ.get('POSTGRES_DB') or 'pep-base-sql',
        'POSTGRES_PORT': os.environ.get('POSTGRES_PORT') or 5432,
        'POSTGRES_HOST': os.environ.get('POSTGRES_HOST') or 'localhost'
    }
