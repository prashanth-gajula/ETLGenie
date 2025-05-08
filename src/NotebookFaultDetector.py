import os
from openai import OpenAI
from dotenv import load_dotenv
from crewai import Agent, Task, Crew
load_dotenv()


api_key=os.getenv("OPENAI_API_KEY")
#print(api_key)

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
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
    )
)


IdentifyFaultyNotebook = Task(
    name="Identify Faulty Notebook",
    description=(
        "Analyze the error message and the main Databricks notebook. Recursively inspect all dependent notebooks "
        "referenced via %run, and determine which notebook is most likely to have caused the error."
    ),
    agent=NotebookLocatorAgent,
    expected_output="The full path of the notebook most closely associated with the error, along with a reasoning summary."
)


crew = Crew(agents=[NotebookLocatorAgent],tasks=[IdentifyFaultyNotebook])
result = crew.kickoff()

print(result)