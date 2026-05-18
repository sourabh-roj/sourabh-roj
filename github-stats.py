#!/usr/bin/python3

import asyncio
import os
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import requests


###############################################################################
# Main Classes
###############################################################################

class Queries(object):
    """
    Class with functions to query the GitHub GraphQL (v4) API and the REST (v3)
    API. Also includes functions to dynamically generate GraphQL queries.
    """

    def __init__(self, username: str, access_token: str,
                 session: aiohttp.ClientSession, max_connections: int = 10):
        self.username = username
        self.access_token = access_token
        self.session = session
        self.semaphore = asyncio.Semaphore(max_connections)

    async def query(self, generated_query: str) -> Dict:
        """
        Make a request to the GraphQL API using the authentication token from
        the environment
        :param generated_query: string query to be sent to the API
        :return: decoded GraphQL JSON output
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        try:
            async with self.semaphore:
                r = await self.session.post("https://api.github.com/graphql",
                                            headers=headers,
                                            json={"query": generated_query})
            return await r.json()
        except Exception:
            print("aiohttp failed for GraphQL query. Falling back to requests.")
            # Fall back on non-async requests safely
            try:
                r = requests.post("https://api.github.com/graphql",
                                  headers=headers,
                                  json={"query": generated_query})
                return r.json()
            except Exception as e:
                print(f"Fallback requests failed for GraphQL: {e}")
                return dict()

    async def query_rest(self, path: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the REST API
        :param path: API path to query
        :param params: Query parameters to be passed to the API
        :return: deserialized REST JSON output
        """
        if params is None:
            params = dict()
        if path.startswith("/"):
            path = path[1:]

        headers = {
            "Authorization": f"token {self.access_token}",
        }

        for _ in range(60):
            try:
                async with self.semaphore:
                    # Pass params directly as a dictionary instead of casting to tuple
                    r = await self.session.get(f"https://api.github.com/{path}",
                                               headers=headers,
                                               params=params)
                if r.status == 202:
                    print(f"A path returned 202. Retrying...")
                    await asyncio.sleep(2)
                    continue

                result = await r.json()
                if result is not None:
                    # Guard clause if the API returns an error dictionary instead of expected data
                    return result if isinstance(result, (dict, list)) else dict()
            except Exception:
                print("aiohttp failed for rest query. Falling back to requests.")
                # Fall back on non-async requests
                try:
                    # Fixed: params passed cleanly and check adjusted to use .status_code
                    r = requests.get(f"https://api.github.com/{path}",
                                     headers=headers,
                                     params=params)
                    if r.status_code == 202:
                        print(f"A path returned 202. Retrying...")
                        await asyncio.sleep(2)
                        continue
                    elif r.status_code == 200:
                        return r.json()
                except Exception as e:
                    print(f"Fallback requests failed for REST: {e}")
                    break

        print("There were too many 202s or errors. Data for this repository will be incomplete.")
        return dict()

    @staticmethod
    def repos_overview(contrib_cursor: Optional[str] = None,
                       owned_cursor: Optional[str] = None) -> str:
        return f"""{{
  viewer {{
    login,
    name,
    repositories(
        first: 100,
        orderBy: {{ field: UPDATED_AT, direction: DESC }},
        isFork: false,
        after: {"null" if owned_cursor is None else '"'+ owned_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
    repositoriesContributedTo(
        first: 100,
        includeUserRepositories: false,
        orderBy: {{ field: UPDATED_AT, direction: DESC }},
        contributionTypes: [ COMMIT, PULL_REQUEST, REPOSITORY, PULL_REQUEST_REVIEW ]
        after: {"null" if contrib_cursor is None else '"'+ contrib_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

    @staticmethod
    def contrib_years() -> str:
        return """
query {
  viewer {
    contributionsCollection {
      contributionYears
    }
  }
}
"""

    @staticmethod
    def contribs_by_year(year: str) -> str:
        return f"""
  year{year}: contributionsCollection(
        from: "{year}-01-01T00:00:00Z",
        to: "{int(year) + 1}-01-01T00:00:00Z"
  ) {{
    contributionCalendar {{
      totalContributions
    }}
  }}
"""

    @classmethod
    def all_contribs(cls, years: List[str]) -> str:
        by_years = "\n".join(map(cls.contribs_by_year, years))
        return f"""
query {{
  viewer {{
    {by_years}
  }}
}}
"""


class Stats(object):
    """
    Retrieve and store statistics about GitHub usage.
    """
    def __init__(self, username: str, access_token: str,
                 session: aiohttp.ClientSession,
                 exclude_repos: Optional[Set] = None,
                 exclude_langs: Optional[Set] = None):
        self.username = username
        self._exclude_repos = set() if exclude_repos is None else exclude_repos
        self._exclude_langs = set() if exclude_langs is None else exclude_langs
        self.queries = Queries(username, access_token, session)

        self._name = None
        self._stargazers = None
        self._forks = None
        self._total_contributions = None
        self._languages = None
        self._repos = None
        self._lines_changed = None
        self._views = None

    async def to_str(self) -> str:
        languages = await self.languages_proportional
        formatted_languages = "\n  - ".join(
            [f"{k}: {v:0.4f}%" for k, v in languages.items()]
        )
        lines_changed = await self.lines_changed
        return f"""Name: {await self.name}
Stargazers: {await self.stargazers:,}
Forks: {await self.forks:,}
All-time contributions: {await self.total_contributions:,}
Repositories with contributions: {len(await self.repos)}
Lines of code added: {lines_changed[0]:,}
Lines of code deleted: {lines_changed[1]:,}
Lines of code changed: {lines_changed[0] + lines_changed[1]:,}
Project page views: {await self.views:,}
Languages:
  - {formatted_languages}"""

    async def get_stats(self) -> None:
        self._stargazers = 0
        self._forks = 0
        self._languages = dict()
        self._repos = set()

        next_owned = None
        next_contrib = None
        while True:
            raw_results = await self.queries.query(
                Queries.repos_overview(owned_cursor=next_owned,
                                       contrib_cursor=next_contrib)
            )
            raw_results = raw_results if raw_results is not None else {}

            self._name = (raw_results
                          .get("data", {})
                          .get("viewer", {})
                          .get("name", None))
            if self._name is None:
                self._name = (raw_results
                              .get("data", {})
                              .get("viewer", {})
                              .get("login", "No Name"))

            contrib_repos = (raw_results
                             .get("data", {})
                             .get("viewer", {})
                             .get("repositoriesContributedTo", {}))
            owned_repos = (raw_results
                           .get("data", {})
                           .get("viewer", {})
                           .get("repositories", {}))
            repos = (contrib_repos.get("nodes", [])
                     + owned_repos.get("nodes", []))

            for repo in repos:
                if not repo:
                    continue
                name = repo.get("nameWithOwner")
                if name in self._repos or name in self._exclude_repos:
                    continue
                self._repos.add(name)
                self._stargazers += repo.get("stargazers", {}).get("totalCount", 0)
                self._forks += repo.get("forkCount", 0)

                for lang in repo.get("languages", {}).get("edges", []):
                    name = lang.get("node", {}).get("name", "Other")
                    if name in self._exclude_langs: 
                        continue
                    if name in self._languages:
                        self._languages[name]["size"] += lang.get("size", 0)
                        self._languages[name]["occurrences"] += 1
                    else:
                        self._languages[name] = {
                            "size": lang.get("size", 0),
                            "occurrences": 1,
                            "color": lang.get("node", {}).get("color")
                        }

            if owned_repos.get("pageInfo", {}).get("hasNextPage", False) or \
                    contrib_repos.get("pageInfo", {}).get("hasNextPage", False):
                next_owned = (owned_repos
                              .get("pageInfo", {})
                              .get("endCursor", next_owned))
                next_contrib = (contrib_repos
                                .get("pageInfo", {})
                                .get("endCursor", next_contrib))
            else:
                break

        langs_total = sum([v.get("size", 0) for v in self._languages.values()])
        if langs_total > 0:
            for k, v in self._languages.items():
                v["prop"] = 100 * (v.get("size", 0) / langs_total)

    @property
    async def name(self) -> str:
        if self._name is not None:
            return self._name
        await self.get_stats()
        return self._name

    @property
    async def stargazers(self) -> int:
        if self._stargazers is not None:
            return self._stargazers
        await self.get_stats()
        return self._stargazers

    @property
    async def forks(self) -> int:
        if self._forks is not None:
            return self._forks
        await self.get_stats()
        return self._forks

    @property
    async def languages(self) -> Dict:
        if self._languages is not None:
            return self._languages
        await self.get_stats()
        return self._languages

    @property
    async def languages_proportional(self) -> Dict:
        if self._languages is None:
            await self.get_stats()
        return {k: v.get("prop", 0) for (k, v) in self._languages.items()}

    @property
    async def repos(self) -> List[str]:
        if self._repos is not None:
            return list(self._repos)
        await self.get_stats()
        return list(self._repos)

    @property
    async def total_contributions(self) -> int:
        if self._total_contributions is not None:
            return self._total_contributions

        self._total_contributions = 0
        res = await self.queries.query(Queries.contrib_years())
        years = res.get("data", {}).get("viewer", {}).get("contributionsCollection", {}).get("contributionYears", [])
        
        if not years:
            return 0
            
        by_year_res = await self.queries.query(Queries.all_contribs(years))
        by_year = by_year_res.get("data", {}).get("viewer", {}).values()
        
        for year in by_year:
            if year:
                self._total_contributions += year.get("contributionCalendar", {}).get("totalContributions", 0)
        return self._total_contributions

    @property
    async def lines_changed(self) -> Tuple[int, int]:
        if self._lines_changed is not None:
            return self._lines_changed
        additions = 0
        deletions = 0
        for repo in await self.repos:
            r = await self.queries.query_rest(f"/repos/{repo}/stats/contributors")
            # If the response is a dict (error message) instead of a stats list, skip it safely
            if isinstance(r, dict) or not isinstance(r, list):
                continue
                
            for author_obj in r:
                if not isinstance(author_obj, dict) or not isinstance(author_obj.get("author", {}), dict):
                    continue
                author = author_obj.get("author", {}).get("login", "")
                if author != self.username:
                    continue

                for week in author_obj.get("weeks", []):
                    additions += week.get("a", 0)
                    deletions += week.get("d", 0)

        self._lines_changed = (additions, deletions)
        return self._lines_changed

    @property
    async def views(self) -> int:
        if self._views is not None:
            return self._views

        total = 0
        for repo in await self.repos:
            r = await self.queries.query_rest(f"/repos/{repo}/traffic/views")
            if isinstance(r, dict):
                for view in r.get("views", []):
                    total += view.get("count", 0)

        self._views = total
        return total


###############################################################################
# Main Function
###############################################################################

async def main() -> None:
    access_token = os.getenv("ACCESS_TOKEN")
    user = os.getenv("GITHUB_ACTOR")
    if not access_token or not user:
        print("Error: Please verify that ACCESS_TOKEN and GITHUB_ACTOR environment variables are set.")
        return
        
    async with aiohttp.ClientSession() as session:
        s = Stats(user, access_token, session)
        print(await s.to_str())


if __name__ == "__main__":
    asyncio.run(main())
