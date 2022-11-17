from jira import JIRA
import os
import json
# from pyarrow import fs
from datetime import date
from plumbum import FG, cmd, local
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor

from string import punctuation

from pyparsing import (
    CaselessLiteral,
    Char,
    Combine,
    FollowedBy,
    Optional,
    ParserElement,
    ParseResults,
    PrecededBy,
    SkipTo,
    StringEnd,
    StringStart,
    Suppress,
    White,
    Word,
    alphanums,
)

import logging
from jira2markdown import convert
from jira2markdown.elements import MarkupElements
from jira2markdown.markup.links import Mention
from jira2markdown.markup.base import AbstractMarkup

logging.basicConfig(level=logging.INFO)

class MigratedMention(AbstractMarkup):
    def action(self, tokens: ParseResults) -> str:
        username = self.usernames.get(tokens.accountid)
        return f"`[~{tokens.accountid}]`" if username is None else f"`[~{username}]`"

    @property
    def expr(self) -> ParserElement:
        MENTION = Combine(
            "["
            + Optional(
                SkipTo("|", failOn="]") + Suppress("|"),
                default="",
                )
            + "~"
            + Optional(CaselessLiteral("accountid:"))
            + Word(alphanums + ":-").setResultsName("accountid")
            + "]",
            )
        return (
                (StringStart() | Optional(PrecededBy(White(), retreat=1), default=" "))
                + MENTION.setParseAction(self.action)
                + (StringEnd() | Optional(FollowedBy(White() | Char(punctuation, excludeChars="[") | MENTION), default=" "))
        )


def main():

    content_list = '''The objective is to list down a set of tasks required to provide UDF support for Apache Arrow streaming execution engine. In the first iteration we will be focusing on providing support for Python-based UDFs which can support Python functions. 

The UDF Integration is going to pan out with a series of sub-tasks associated with the development and PoCs. Note that this is going to be the first iteration of UDF integrations with a limited scope. This ticket will cover the following topics;
# POC for UDF integration: The objective is to evaluate the existing components in the source and evaluate the required modifications and new building blocks required to integrate UDFs.
# The language will be limited to C+{+}/{+}Python users can register Python function as a UDF and use it with an `apply` method on Arrow Tables or provide a computation API endpoint via arrow::compute API. Note that the C+ API already provides a way to register custom functions via the function registry API. At the moment this is not exposed to Python. 
# Planned features for this ticket are;
## Scalar UDFs : UDFs executed per value (per row)
## Vector UDFs : UDFs executed per batch (a full array or partial array)
## Aggregate UDFs : UDFs associated with an aggregation operation
# Integration limitations
## Doesn't support custom data types which doesn't support Numpy or Pandas
## Complex processing with parallelism within UDFs are not supported
## Parallel UDFs are not supported in the initial version of UDFs. Allthough we are documenting what is required and a rough sketch for the next phase.'''

    content = '''[~toddfarmer] Thank you for your work here! With this and the other configuration options you've added, I believe this allows for a reasonable remedy of the Postgres unbounded decimal issue (with variable scale). 

I think my best course of action will be to detect these columns (by inspecting the result set ahead of time) and using the tooling you've provided to map unbounded decimal values to a reasonable precision / scale.
'''

    elements = MarkupElements()
    elements.replace(Mention, MigratedMention)
    logging.info(convert(content_list, elements=elements))

if __name__ == '__main__':
    main()
