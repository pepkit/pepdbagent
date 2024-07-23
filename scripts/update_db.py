import os

from dotenv import load_dotenv
from tqdm import tqdm

from pepdbagent import PEPDatabaseAgent

load_dotenv()


def update():
    agent = PEPDatabaseAgent(
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST"),
        database=os.environ.get("POSTGRES_DB"),
        # port=os.environ.get("POSTGRES_PORT"),
    )

    if_list = agent.update.get_namespace_projects("geo")

    for i in tqdm(if_list, desc="Updating projects"):
        agent.update.update_parent_project(i)


if __name__ == "__main__":
    update()
