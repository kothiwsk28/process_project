#!/usr/bin/env python3
import os
from collections import defaultdict
from datetime import datetime
from itertools import chain
from string import Template
from typing import Dict, List, Optional, Tuple, TypeVar, Union

import attr
import cattr
import click
import requests  # type: ignore

GITHUB_TOKEN = "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"


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class PageInfo:
    hasNextPage: bool
    endCursor: str


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Label:
    name: str


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class LabelConnection:
    totalCount: int
    pageInfo: PageInfo
    nodes: List[Label]


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class User:
    login: str
    name: str


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class UserConnection:
    totalCount: int
    pageInfo: PageInfo
    nodes: List[User]


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Issue:
    id: str
    url: str
    title: str
    closed: bool
    closedAt: Optional[datetime] = None
    assignees: Optional[UserConnection] = None
    labels: Optional[LabelConnection] = None


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class IssueConnection:
    totalCount: int
    nodes: List[Issue]
    pageInfo: Optional[PageInfo] = None


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class PullRequest:
    id: str
    title: str
    closed: bool
    merged: bool
    closedAt: Optional[datetime] = None
    closingIssuesReferences: Optional[IssueConnection] = None


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class PullRequestConnection:
    totalCount: int
    pageInfo: PageInfo
    nodes: List[PullRequest]


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Card:
    id: str
    isArchived: bool
    url: str
    note: Optional[str] = None
    content: Optional[Union[PullRequest, Issue]] = None


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Repo:
    name: str
    issues: Optional[IssueConnection] = None
    pullRequests: Optional[PullRequestConnection] = None


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class CardConnection:
    totalCount: int
    pageInfo: PageInfo
    nodes: List[Card]


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Column:
    id: str
    name: str
    cards: CardConnection


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class ColumnConnection:
    totalCount: int
    pageInfo: PageInfo
    nodes: List[Column]


@attr.s(auto_attribs=True, on_setattr=attr.setters.frozen)
class Project:
    id: str
    name: str
    pendingCards: CardConnection
    columns: ColumnConnection


def structure_datetime(dt, _):
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, str):
        return datetime.fromisoformat(dt.rstrip("Z"))
    raise Exception(f"Cannot structure the input value {dt} with the type {type(dt)}")


def unstructure_datetime(dt: datetime):
    dt.isoformat()


def structure_content(o, _):
    clazz = PullRequest if o["__typename"] == "PullRequest" else Issue
    return cattr.structure(o, clazz)


cattr.register_structure_hook(Union[PullRequest, Issue], structure_content)
cattr.register_unstructure_hook(datetime, unstructure_datetime)
cattr.register_structure_hook(datetime, structure_datetime)


PROJECT_QUERY_TEMPLATE = Template(
    """
  $account_type(login: "$owner") {
    id
    project(number: $project_number) {
      id
      name
      pendingCards(first: 100) {
        totalCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          isArchived
          url
          note
          content {
            __typename
            ... on Issue {
              id
              url
              title
              closed
              closedAt
            }
            ... on PullRequest {
              id
              title
              closed
              closedAt
              merged
              closingIssuesReferences(first: 5) {
                totalCount
                nodes {
                  id
                  url
                  title
                  closed
                  closedAt
                }
              }
            }
          }
        }
      }
      columns(first: 15) {
        totalCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          name
          cards(first: 100) {
            totalCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              id
              isArchived
              url
              note
              content {
                __typename
                ... on Issue {
                  id
                  url
                  title
                  closed
                  closedAt
                }
                ... on PullRequest {
                  id
                  title
                  closed
                  closedAt
                  merged
                  closingIssuesReferences(first: 5) {
                    totalCount
                    pageInfo {
                      hasNextPage
                      endCursor
                    }
                    nodes {
                      id
                      url
                      title
                      closed
                      closedAt
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
"""
)

QUERY_TEMPLATE = Template(
    """
query {
    $queries
}
"""
)

PULLREQUEST_CONTENTS = """
    pullRequests(first: 100, after: $after) {
      totalCount
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        title
        closed
        closedAt
        merged
        closingIssuesReferences(first: 5) {
          totalCount
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            url
            title
            closed
            closedAt
          }
        }
      }
    }
"""

ISSUE_CONTENTS = """
    issues(first: 100, after: $after) {
      totalCount
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        url
        title
        closed
        closedAt
        assignees(first: 10) {
          totalCount
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            login
            name
          }
        }
        labels(first:10) {
          totalCount
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            name
          }
        }
      }
    }
"""

INITIAL_REPO_QUERY_TEMPLATE = Template(
    Template(
        """
  $repo_name_underscore : repository(owner:"$owner", name: "$repo_name") {
    name
    $issue_contents
    $pr_contents
  }
"""
    ).safe_substitute(pr_contents=PULLREQUEST_CONTENTS, issue_contents=ISSUE_CONTENTS)
)

MORE_REPO_ISSUES_QUERY_TEMPLATE = Template(
    Template(
        """
  $repo_name_underscore : repository(owner:"$owner", name: "$repo_name") {
    name
    $issue_contents
  }
"""
    ).safe_substitute(issue_contents=ISSUE_CONTENTS)
)

MORE_REPO_PULLREQUESTS_QUERY_TEMPLATE = Template(
    Template(
        """
  $repo_name_underscore : repository(owner:"$owner", name: "$repo_name") {
    name
    $pr_contents
  }
"""
    ).safe_substitute(pr_contents=PULLREQUEST_CONTENTS)
)

MORE_REPO_ISSUES_QUERY_TEMPLATE = Template(
    Template(
        """
  $repo_name_underscore : repository(owner:"$owner", name: "$repo_name") {
    name
    $issue_contents
  }
"""
    ).safe_substitute(issue_contents=ISSUE_CONTENTS)
)

CARD_MOVE_MUTATION_TEMPLATE = Template(
    """
  moveProjectCard(input: {
    cardId: "$card_id",
    columnId: "$column_id",
    afterCardId: $after_card_id,
    clientMutationId: "$card_id"
  }) {
    clientMutationId
  }
"""
)

CARD_ADD_MUTATION_TEMPLATE = Template(
    """
  addProjectCard(input: {
    projectColumnId: "$column_id",
    contentId: "$content_id",
    clientMutationId: "$content_id"
  }) {
    clientMutationId
    cardEdge {
      node {
        id
      }
    }
  }
"""
)

CARD_DELETE_MUTATION_TEMPLATE = Template(
    """
  deleteProjectCard(input: {
    clientMutationId: "1",
    cardId: "$card_id"
  }) {
    clientMutationId
  }
"""
)

CARD_UPDATE_MUTATION_TEMPLATE = Template(
    """
  updateProjectCard(input: {
    projectCardId: "$card_id",
    note:"$note",
    clientMutationId: "$card_id"
  })
  {
    clientMutationId
  }
"""
)

MUTATION_TEMPLATE = Template(
    """
mutation {
    $mutations
}
"""
)


def get_config(test_board: bool) -> Tuple:
    # if test_board:
    board_name = "test_board"
    repos = ["process_project"]
    done_age_out = 7
    project_number = 3
    account_owner = "kothiwsk28"
    account_type = "user"
    """
    else:
        board_name = "Data Science"
        repos = [
            "core",
            "reference-data",
            "unified-asset",
            "directory-pipeline",
            "demand-forecast",
            "care-pathways",
            "trilliant-ml-ops",
            "cookiecutter",
            "provider-affiliation",
            "mldemo",
            "similarity",
        ]
        done_age_out = 7
        project_number = 5
        account_owner = "TrilliantHealth"
        account_type = "organization"
    """
    return board_name, repos, done_age_out, project_number, account_type, account_owner


ITEM = TypeVar("ITEM")


def get_content_ids(cards: List[Card]) -> Dict[str, Card]:
    card_content_ids = {card.content.id: card for card in cards if card.content is not None}
    return card_content_ids


def run_query(query: str) -> dict:
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    result = requests.post("https://api.github.com/graphql", json={"query": query}, headers=headers)
    if result.status_code == 200:
        result_json = result.json()
        assert "data" in result_json, f"No data found in response: {result_json}"
        return result.json()["data"]
    else:
        raise Exception(
            "Query failed to run by returning code of {}. {}".format(result.status_code, query)
        )


def render_nullable_string(value: Optional[str] = None) -> str:
    return "null" if value is None else f'"{value}"'


def move_issue(
    item: Union[Issue, PullRequest],
    column: Column,
    column_ids: Dict[str, Card],
    last_note_id: Optional[str],
    card_content_ids: Dict[str, Card],
):
    try:
        if item.id in column_ids:
            pass
        elif item.id in card_content_ids:
            print(f"Moving issue ({item}) to {column.name}")
            card = card_content_ids[item.id]
            mutations = CARD_MOVE_MUTATION_TEMPLATE.substitute(
                card_id=card.id, column_id=column.id, after_card_id=render_nullable_string(last_note_id)
            )
            mutation = MUTATION_TEMPLATE.substitute(mutations=mutations)
            run_query(mutation)
        else:
            print(f"Adding issue ({item}) to {column.name}")
            # first add issue
            mutations = CARD_ADD_MUTATION_TEMPLATE.substitute(content_id=item.id, column_id=column.id)
            mutation = MUTATION_TEMPLATE.substitute(mutations=mutations)
            add_result = run_query(mutation)

            # then move issue after notes
            card_id = add_result["addProjectCard"]["cardEdge"]["node"]["id"]
            move_card(card_id, column, last_note_id)

    except Exception as e:
        print(f"Error moving issue {item} with exception: {e}")


def move_card(
    card_id: str,
    column: Column,
    after_card_id: Optional[str] = None,
):
    try:
        mutations = CARD_MOVE_MUTATION_TEMPLATE.substitute(
            card_id=card_id, column_id=column.id, after_card_id=render_nullable_string(after_card_id)
        )
        mutation = MUTATION_TEMPLATE.substitute(mutations=mutations)
        run_query(mutation)
    except Exception as e:
        print(f"Error moving card {card_id} with exception: {e}")


def remove_issue(item: Union[Issue, PullRequest], card_content_ids: Dict[str, Card]):
    try:
        if item.id in card_content_ids:
            print(f"Removing issue ({item}) from project board.")
            card = card_content_ids[item.id]
            mutations = CARD_DELETE_MUTATION_TEMPLATE.substitute(card_id=card.id)
            mutation = MUTATION_TEMPLATE.substitute(mutations=mutations)
            run_query(mutation)
    except Exception as e:
        print(f"Error moving issue {item} with exception: {e}")


def get_repo_prs(repo: Repo, account_owner: str, account_type: str) -> List[PullRequest]:
    repo_name_underscore = repo.name.replace("-", "_")
    current_repo = repo
    assert (
        current_repo.pullRequests is not None
    ), "Expected Repo instance to have a PullRequestConnection"
    prs = list(current_repo.pullRequests.nodes)
    while current_repo.pullRequests.pageInfo.hasNextPage:
        print("getting more pages")

        more_query = MORE_REPO_PULLREQUESTS_QUERY_TEMPLATE.substitute(
            repo_name=repo.name,
            repo_name_underscore=repo_name_underscore,
            owner=account_owner,
            account_type=account_type,
            after=f'"{current_repo.pullRequests.pageInfo.endCursor}"',
        )
        query = QUERY_TEMPLATE.substitute(queries=more_query)
        result = run_query(query)
        current_repo = cattr.structure(result[repo_name_underscore], Repo)
        assert (
            current_repo.pullRequests is not None
        ), "Expected Repo instance to have a PullRequestConnection"
        prs.extend(current_repo.pullRequests.nodes)
    return prs


def get_repo_issues(repo: Repo, account_owner: str, account_type: str) -> List[Issue]:
    repo_name_underscore = repo.name.replace("-", "_")
    current_repo = repo
    assert current_repo.issues is not None, "Expected Repo instance to have a IssueConnection"
    issues = list(current_repo.issues.nodes)
    if current_repo.issues.pageInfo:
        while current_repo.issues.pageInfo.hasNextPage:
            print("getting more pages")
            more_query = MORE_REPO_ISSUES_QUERY_TEMPLATE.substitute(
                repo_name=repo.name,
                repo_name_underscore=repo_name_underscore,
                owner=account_owner,
                account_type=account_type,
                after=f'"{current_repo.issues.pageInfo.endCursor}"',
            )
            query = QUERY_TEMPLATE.substitute(queries=more_query)
            result = run_query(query)
            current_repo = cattr.structure(result[repo_name_underscore], Repo)
            assert current_repo.issues is not None, "Expected Repo instance to have a IssueConnection"
            issues.extend(current_repo.issues.nodes)
    return issues


def move_notes(column: Column) -> Optional[str]:
    """
    Helper function to move all notes to top of Column.

    Returns Card id of last Note
    """
    last_note_card_id: Optional[str] = None
    last_content_card_id = None
    for card in column.cards.nodes:
        if card.note is not None:
            if last_content_card_id is not None:
                move_card(card.id, column, after_card_id=last_note_card_id)
            last_note_card_id = card.id
        else:
            last_content_card_id = card.id
    return last_note_card_id


def run_initial_query(
    account_owner: str, account_type: str, project_number: int, repo_names: List[str]
) -> Tuple[Project, List[Repo]]:
    # run initial query
    repo_queries = [
        INITIAL_REPO_QUERY_TEMPLATE.substitute(
            repo_name=repo_name,
            repo_name_underscore=repo_name.replace("-", "_"),
            owner=account_owner,
            after="null",
        )
        for repo_name in repo_names
    ]
    project_query = PROJECT_QUERY_TEMPLATE.substitute(
        owner=account_owner, account_type=account_type, project_number=project_number
    )
    query = QUERY_TEMPLATE.substitute(queries="\n".join([project_query, *repo_queries]))
    print(query)
    json_result = run_query(query)
    print(json_result)
    repos = [cattr.structure(repo, Repo) for name, repo in json_result.items() if name != account_type]
    project: Project = cattr.structure(json_result[account_type]["project"], Project)
    return project, repos


def get_column(project_result: Project, column_name: str) -> Tuple[Column, Dict[str, Card]]:
    assert (
        len(project_result.columns.nodes) == project_result.columns.totalCount
    ), "Need to query for more columns"
    columns = {column.name: column for column in project_result.columns.nodes}
    column = columns[column_name]

    # get project cards
    cards = column.cards.nodes
    assert len(cards) == column.cards.totalCount, f"Need to query for more {column_name} cards"
    card_ids: Dict[str, Card] = get_content_ids(cards)
    return column, card_ids


def get_issues_to_prs(pull_requests: List[PullRequest]) -> Dict[str, List[PullRequest]]:
    issues_for_prs = [
        (issue, pr)
        for pr in pull_requests
        if (pr.closingIssuesReferences is not None and pr.closingIssuesReferences.totalCount > 0)
        for issue in pr.closingIssuesReferences.nodes
    ]
    issue_to_prs = defaultdict(list)
    for issue, pr in issues_for_prs:
        issue_to_prs[issue.id].append(pr)
    return issue_to_prs


def get_pitch_status(account_owner: str, account_type: str) -> str:
    print("Get Pitch Status")
    project_result, repos = run_initial_query(
        account_owner=account_owner,
        account_type=account_type,
        project_number=11,
        repo_names=["product"],
    )
    issues = list(chain(*(get_repo_issues(repo, account_owner, account_type) for repo in repos)))
    issue_ids = {issue.id: issue for issue in issues}

    active_pitches = list()
    assert not project_result.columns.pageInfo.hasNextPage, "Must query for more columns"
    for column in project_result.columns.nodes:
        assert not column.cards.pageInfo.hasNextPage, "Must query for more cards"
        if column.name.lower().strip() != "in progress":
            continue
        for card in column.cards.nodes:
            if card.content is not None and isinstance(card.content, Issue):
                issue = issue_ids[card.content.id]
                assert issue.labels is not None, "Expected to find labels for issue"
                assert not issue.labels.pageInfo.hasNextPage, "Must query for more labels"
                for label in issue.labels.nodes:
                    assert issue.assignees is not None, "Expected to find assignees for issue"
                    if label.name == "DS" and issue.assignees.totalCount > 0:
                        active_pitches.append(issue)

    messages = list()
    for issue in active_pitches:
        assert issue.assignees is not None, "Expected to find assignees for issue"
        owners = ", ".join([user.login for user in issue.assignees.nodes])
        message = f"[{issue.title}]({issue.url})\nOwners: {owners}"
        messages.append(message)
    return "\n\n".join(messages)


def set_pitch_status_message(project_result: Project, message: str):
    notes_column, _ = get_column(project_result, "Notes")
    for card in notes_column.cards.nodes:
        if card.note is not None and card.note.startswith("# Active Pitches"):
            update_mutation = CARD_UPDATE_MUTATION_TEMPLATE.substitute(
                card_id=card.id, note=f"# Active Pitches\n\n{message}"
            )
            query = MUTATION_TEMPLATE.substitute(mutations=update_mutation)
            run_query(query)


@click.group()
def main():
    pass


@click.command()
@click.option(
    "--test-board",
    type=bool,
    default=False,
    is_flag=True,
    help="Set flag to use test project board",
)
def run(test_board: bool):
    board_name, repo_names, done_age_out, project_number, account_type, account_owner = get_config(
        test_board
    )
    print("Running Board Update with config:")
    print(f"  board = {board_name}")
    print(f"  repo_names = {repo_names}")
    print(f"  done_age_out = {done_age_out}")

    # run initial query
    project_result, repos = run_initial_query(
        account_owner=account_owner,
        account_type=account_type,
        project_number=project_number,
        repo_names=repo_names,
    )
    print(f"Found {len(repos)} repos")

    # get list of issues
    issues = list(chain(*(get_repo_issues(repo, account_owner, account_type) for repo in repos)))
    print(f"Found {len(issues)} issues")

    # get list of pull requests
    pull_requests = list(chain(*(get_repo_prs(repo, account_owner, account_type) for repo in repos)))
    print(f"Found {len(pull_requests)} pull requests")

    # get project cards
    print("Getting Current Project Board Cards")
    pending_cards = project_result.pendingCards.nodes
    assert (
        len(pending_cards) == project_result.pendingCards.totalCount
    ), "Need to query for more Pending cards"

    # TODO: turn totalCount assertions into additional queries
    pending_ids: Dict[str, Card] = get_content_ids(pending_cards)
    todo_column, todo_ids = get_column(project_result, "To Do")
    in_progress_column, in_progress_ids = get_column(project_result, "In Progress")
    done_column, done_ids = get_column(project_result, "Done")
    card_content_ids: Dict[str, Card] = {**todo_ids, **in_progress_ids, **done_ids, **pending_ids}
    print(f"Found {len(card_content_ids)} cards")

    print("Get Issues for PRs")
    issue_to_prs = get_issues_to_prs(pull_requests)

    print("Updating Project Board")
    print("Moving Notes to Top")
    last_note_ids = {column.id: move_notes(column) for column in project_result.columns.nodes}

    print("Updating Issues")
    for issue in issues:
        if issue.closed:
            assert (
                issue.closedAt is not None
            ), "Unexpected condition encountered Issue is closed yet closedAt is None"
            closed_age = datetime.now() - issue.closedAt
            if closed_age.days > done_age_out:
                remove_issue(issue, card_content_ids)
            else:
                move_issue(issue, done_column, done_ids, last_note_ids[done_column.id], card_content_ids)
        else:
            is_assigned = issue.assignees is not None and issue.assignees.totalCount > 0
            has_pr = issue.id in issue_to_prs
            if is_assigned or has_pr:
                move_issue(
                    issue,
                    in_progress_column,
                    in_progress_ids,
                    last_note_ids[in_progress_column.id],
                    card_content_ids,
                )
            else:
                move_issue(issue, todo_column, todo_ids, last_note_ids[todo_column.id], card_content_ids)

    print("Updating PRs")
    for pr in pull_requests:
        if pr.closed:
            assert (
                pr.closedAt is not None
            ), "Unexpected condition encountered PullRequest is closed yet closedAt is None"
            closed_age = datetime.now() - pr.closedAt
            if closed_age.days > done_age_out:
                remove_issue(pr, card_content_ids)
            elif pr.merged:
                move_issue(pr, done_column, done_ids, last_note_ids[done_column.id], card_content_ids)
            else:
                remove_issue(pr, card_content_ids)
        else:
            move_issue(
                pr,
                in_progress_column,
                in_progress_ids,
                last_note_ids[in_progress_column.id],
                card_content_ids,
            )

    message = get_pitch_status(account_owner, account_type)
    set_pitch_status_message(project_result, message)


main.add_command(run)

if __name__ == "__main__":
    main()
