import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # Graph RAG using Text2Cypher

    This is a demo app in marimo that allows you to query the Nobel laureate graph (that's managed in Kuzu) using natural language. A language model takes in the question you enter, translates it to Cypher via a custom Text2Cypher pipeline in Kuzu that's powered by DSPy. The response retrieved from the graph database is then used as context to formulate the answer to the question.

    > \- Powered by Kuzu, DSPy and marimo \-
    """
    )
    return


@app.cell
def _(mo):
    text_ui = mo.ui.text(
        value="Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
        full_width=True,
    )
    use_knn_checkbox = mo.ui.checkbox(
        value=True,
        label="Use KNN few-shot (dynamic exemplar selection)"
    )
    use_validation_checkbox = mo.ui.checkbox(
        value=True,
        label="Use self-refinement (generate → validate → repair, max 3 iterations)"
    )
    return text_ui, use_knn_checkbox, use_validation_checkbox


@app.cell
def _(mo, text_ui, use_knn_checkbox, use_validation_checkbox):
    mo.vstack([text_ui, use_knn_checkbox, use_validation_checkbox])
    return


@app.cell
def _(KuzuDatabaseManager):
    db_name = "nobel.kuzu"
    db_manager = KuzuDatabaseManager(db_name)
    return db_manager,


@app.cell
def _(create_graph_rag, use_knn_checkbox, use_validation_checkbox):
    # Create GraphRAG instance based on checkbox states
    # This cell only re-runs when checkboxes change
    use_knn = use_knn_checkbox.value
    use_validation = use_validation_checkbox.value
    rag_instance = create_graph_rag(use_knn=use_knn, use_validation=use_validation, k=3)
    return rag_instance, use_knn, use_validation


@app.cell
def _(db_manager, mo, rag_instance, run_graph_rag, text_ui, use_knn, use_validation):
    question = text_ui.value

    with mo.status.spinner(title="Generating answer...") as _spinner:
        result = run_graph_rag(
            [question],
            db_manager,
            rag_instance=rag_instance,
            use_knn=use_knn,
            use_validation=use_validation
        )[0]

    query = result["query"]
    answer = result["answer"].response
    return answer, query


@app.cell
def _(answer, mo, query):
    mo.hstack(
        [mo.md(f"""### Query\n```{query}```"""), mo.md(f"""### Answer\n{answer}""")]
    )
    return


@app.cell
def _(graph_rag_lib):
    # Import DSPy signatures from the library module
    PruneSchema = graph_rag_lib.PruneSchema
    Text2Cypher = graph_rag_lib.Text2Cypher
    AnswerQuestion = graph_rag_lib.AnswerQuestion
    return AnswerQuestion, PruneSchema, Text2Cypher


@app.cell
def _(BAMLAdapter, OPENROUTER_API_KEY, dspy):
    # Using OpenRouter. Switch to another LLM provider as needed
    lm = dspy.LM(
        model="openrouter/google/gemini-2.5-flash",
        api_base="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )
    try:
        dspy.configure(lm=lm, adapter=BAMLAdapter())
    except RuntimeError:
        pass
    return


@app.cell
def _(graph_rag_lib):
    # Import database manager from the library module
    KuzuDatabaseManager = graph_rag_lib.KuzuDatabaseManager
    return (KuzuDatabaseManager,)


@app.cell
def _(graph_rag_lib):
    # Import data models from the library module
    Query = graph_rag_lib.Query
    GraphSchema = graph_rag_lib.GraphSchema
    return GraphSchema, Query


@app.cell
def _(graph_rag_lib):
    # Import GraphRAG functions from the library
    create_graph_rag = graph_rag_lib.create_graph_rag
    run_graph_rag = graph_rag_lib.run_graph_rag
    return create_graph_rag, run_graph_rag


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    import os
    from pathlib import Path

    import dspy
    import kuzu
    from dotenv import load_dotenv
    from dspy.adapters.baml_adapter import BAMLAdapter
    from pydantic import BaseModel, Field

    import graph_rag_lib

    load_dotenv()

    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
    return (
        BAMLAdapter,
        BaseModel,
        Field,
        OPENROUTER_API_KEY,
        Path,
        dspy,
        graph_rag_lib,
        kuzu,
        mo,
    )


if __name__ == "__main__":
    app.run()
