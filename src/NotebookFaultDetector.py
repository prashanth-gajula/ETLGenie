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
from UpdateDataBricksNotebook import upload_notebook_to_databricks
class NoteBookPathAndError(BaseModel):
    Path: str
    Code: str
    
class FixedCode(BaseModel):
    Fix: str
    updated_full_code: str

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

@tool("UploadNotebook")
def UpdateNotebook(Path: str, source_code: str) -> str:
    """Fetch and return the source code of the Databricks notebook given a full workspace path."""
    return upload_notebook_to_databricks(Path,source_code)

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

#Second Agent and task that will suggest a fix for the line of code that is causing the error.

FixSuggestionAgent = Agent(
    name="FixSuggestionAgent",
    role="Code Fix Generator",
    goal="Generate a valid Spark fix for the line of code causing the error.",
    description="You will use the error context and faulty code line to generate a corrected line of Spark code.",
    backstory="You're a Spark syntax expert capable of correcting DataFrame logic and column references.",
    llm=LLMAgent,
    tools=[FetchSourceCode],
    verbose=True
)

FixErrorLineTask = Task(
    name="Fix Error Line",
    description=(
        "You are a debugging agent analyzing a Databricks notebook failure.\n\n"
        "**You are provided with:**\n"
        "- The full notebook path that contains the error: {{ notebook_path }}\n"
        "- The original line of code that caused the failure: {{ error_line }}\n"
        "- The error message generated during execution: {{ error_message }}\n\n"

        "**Your goal is to:**\n"
        "1. Call the tool `FetchSourceCode(path='{{ notebook_path }}')` to retrieve the full notebook source code.\n"
        "2. Store the result of the tool (i.e., the full source code) in memory.\n"
        "3. Search for the line `{{ error_line }}` in that notebook code.\n"
        "4. Replace the faulty line with a corrected version based on the error message.\n"
        "5. Reconstruct the notebook with the fix applied in place of the original line.\n\n"

        "**Instructions for Output:**\n"
        "- Return a JSON with the following structure:\n"
        "  {\n"
        "    \"suggested_fix\": \"<corrected_line_of_code>\",\n"
        "    \"updated_full_code\": \"<entire_fixed_notebook_code>\"\n"
        "  }\n"
        "- Do not suggest unrelated changes.\n"
        "- Do not include markdown, explanation, or comments.\n"
        "- Make sure the original structure of the notebook is preserved in `updated_full_code`.\n"
    ),
    agent=FixSuggestionAgent,
    expected_output=(
        "A JSON object with the corrected line as `suggested_fix` and the fully updated notebook code as `updated_full_code`."
    ),
    context=[IdentifyFaultyNotebook],  # Make sure this task runs after your locator
    output_json=FixedCode
)

#Agent to fix the notebook code
UploadAgent = Agent(
    name="NotebookUploaderAgent",
    role="Databricks Notebook Publisher",
    goal="Update the Databricks workspace with the newly fixed notebook code.",
    backstory="You are a deployment automation agent responsible for publishing updated notebook code.",
    description="Takes the updated notebook source code and uploads it to Databricks using the workspace/import API.",
    tools=[UpdateNotebook],
    verbose=True
)

UploadNotebookTask = Task(
    name="Publish Fixed Notebook",
    description=(
    "You are the final publishing agent in a multi-step debugging pipeline.\n\n"
    "**Context:**\n"
    "- The task `IdentifyFaultyNotebook` has already identified the notebook path (`Path`) that caused the error.\n"
    "- The task `FixErrorLineTask` has already provided the corrected full source code (`updated_full_code`) for that notebook.\n\n"
    "**Your Responsibilities:**\n"
    "1. Use the notebook path returned from the first task (`IdentifyFaultyNotebook`).\n"
    "2. Use the updated full notebook code returned from the second task (`FixErrorLineTask`).\n"
    "3. Call the tool `UploadNotebook(path=..., source_code=...)` using those values.\n"
    "4. Ensure the notebook is updated in the Databricks workspace successfully.\n\n"
    "**Constraints:**\n"
    "- Do not modify the updated code.\n"
    "- Do not generate new logic.\n"
    "- Simply upload the notebook as-is using the provided path and source code.\n\n"
    "**Output:**\n"
    "Return only the success or failure message from the upload tool execution."
),
    agent=UploadAgent,
    expected_output="A confirmation that the notebook was successfully uploaded to Databricks.",
    context=[IdentifyFaultyNotebook,FixErrorLineTask]
) 


if __name__ == "__main__":
    crew = Crew(
            agents=[NotebookLocatorAgent,FixSuggestionAgent,UploadAgent],
            tasks=[IdentifyFaultyNotebook,FixErrorLineTask,UploadNotebookTask],
            knowledge_sources=[Error_source,code_source,path_source],
            process_type="sequential"
            )
    result = crew.kickoff()
    #print("NoteBook Path:",result["Path"])
    #print("Code That Caused the error:",result["Code"])
    #task_output = IdentifyFaultyNotebook.output
    #print(task_output.json_dict)
    print(result)