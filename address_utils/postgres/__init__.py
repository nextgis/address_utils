
from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.declarative import declarative_base



DBSession = sessionmaker()
Base = declarative_base()

