import os
from openai import OpenAI
from dotenv import load_dotenv
from crewai import Agent, Task, Crew,LLM
from crewai.knowledge.source.string_knowledge_source import StringKnowledgeSource
from crewai.tools import tool
from pydantic import BaseModel

load_dotenv()
os.environ['OPENAI_API_KEY']
LLMAgent= LLM(model = 'gpt-4',temperature = 0)

from NoteBookReader import fetch_notebook_source
from DatabricksJobManager import (
    list_databricks_jobs,
    get_latest_job_run_id,
    get_task_run_ids
)
from get_error_message import get_error_message_from_run_output
class NoteBookPathAndError(BaseModel):
    Path: str
    Code: str

#Fetching necessary Id's to read the error message and the main Notebook path 
#Get the Job_Id of the latest job
Job_Id = list_databricks_jobs()
#get latest JobRunId for that Job
Job_Run_Id = get_latest_job_run_id(Job_Id)
#Get Task run Id for that job run Id
Task_run_id = get_task_run_ids(Job_Run_Id.get("job_run_id"))

#Fetching Actual Error and the file path of the main notebook
Error_And_Path = get_error_message_from_run_output(Task_run_id)
Path = Error_And_Path.get("Path")
#Fetching the Actual SourceCode of the main not book with the help of notebook path that we have received from previous step.
SourceCode = fetch_notebook_source(Path)
@tool("FetchSourceCode")
def FetchSourceCode(Path: str) -> str:
    """Fetch and return the source code of the Databricks notebook given a full workspace path."""
    return fetch_notebook_source(Path)

Error_Message = Error_And_Path.get("error_message")

Error_source = StringKnowledgeSource(
    content=Error_Message,
)

code_source = StringKnowledgeSource(
    content=SourceCode,
)

path_source = StringKnowledgeSource(
    content=Path,
)

NotebookLocatorAgent = Agent(
    name="NotebookLocatorAgent",
    role="Notebook Dependency Analyzer",
    goal="Identify the Databricks notebook most likely responsible for the reported error.",
    description=(
        "An intelligent agent designed to analyze a Databricks workflow notebook and recursively trace all "
        "referenced notebooks (via %run). This agent matches the error context with notebook content to pinpoint "
        "the most relevant notebook associated with the failure."
    ),
    backstory=(
        "You are an AI debugging assistant specialized in tracing the root cause of pipeline failures in complex "
        "Databricks workflows. You navigate through notebook dependencies to identify which component most likely "
        "caused the issue based on the error message."
    ),
    LLM = LLMAgent,
    tools=[FetchSourceCode],
    verbose=True
)


IdentifyFaultyNotebook = Task(
    name="Identify Faulty Notebook",
    description=(
    "You are a debugging agent analyzing Databricks notebook failures.\n"
    "Inputs:\n"
    "- Error message: {{ error_message }}\n"
    "- Main notebook source code: {{ main_notebook_code }}\n"
    "- Main notebook path: {{ main_path }}\n\n"
    "**Instructions:**\n"
    "1. Scan the main notebook for any `%run` statements.\n"
    "2. If the `%run` references a relative path like `./reader_factory`, you must convert it to a full path.\n"
    "3. To do this, use the main notebook path `{{ main_path }}` and append the notebook name.\n"
    "   - Example: if `%run ./reader_factory`, and `main_path = /Workspace/Users/kumargajula782@gmail.com/AppleAnalysis`,\n"
    "     then full path = `/Workspace/Users/kumargajula782@gmail.com/AppleAnalysis/reader_factory`\n"
    "4. Once you’ve built the full path, call the tool `FetchSourceCode(path='...')` with that full path.\n"
    "5. Analyze the retrieved notebook source to determine if it contains logic related to the error.\n\n"
    "**Important:** Only call `FetchSourceCode()` using paths that start with `/`. Do not call it with relative paths."
),
    agent=NotebookLocatorAgent,
    expected_output="The full path of the notebook most closely associated with the error, along with the line of code that caused the potential error",
    input_variables={"path":Path},
    output_json=NoteBookPathAndError
)


if __name__ == "__main__":
    crew = Crew(
            agents=[NotebookLocatorAgent],
            tasks=[IdentifyFaultyNotebook],
            knowledge_sources=[Error_source,code_source,path_source]
            )
    result = crew.kickoff()
    print("NoteBook Path:",result["Path"])
    print("Code That Caused the error:",result["Code"])
    #task_output = IdentifyFaultyNotebook.output
    #print(task_output.json_dict)