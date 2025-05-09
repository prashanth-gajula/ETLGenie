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
        "You are provided with:\n"
        "- The full notebook path that contains the error: {{ notebook_path }}\n"
        "- The original line of code that caused the failure: {{ error_line }}\n"
        "- The error message generated during execution: {{ error_message }}\n\n"

        "**Instructions:**\n"
        "1. Call the tool `FetchSourceCode(path='{{ notebook_path }}')` to retrieve the full source code of the notebook.\n"
        "2. Analyze the full source code, especially the surrounding context of the failing line.\n"
        "3. Understand the likely reason for the error using the notebook's logic and the error message.\n"
        "4. Suggest a corrected version of the failing line.\n\n"

        "**Constraints:**\n"
        "- Only return the corrected line as a JSON object like this:\n"
        "  {\"suggested_fix\": \"<your_fixed_code_here>\"}\n"
        "- Do not suggest changes to unrelated code.\n"
        "- Do not include commentary or markdown.\n"
    ),
    agent=FixSuggestionAgent,
    expected_output="A JSON with a single key `suggested_fix` containing the corrected Spark code line.",
    context = [IdentifyFaultyNotebook],
    output_json=FixedCode
)