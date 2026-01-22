import pandas as pd

from unspsc_classifier import (
    OpenAICompletionClient,
    UnspscClassifier,
    ensure_columns,
    process_batches,
)


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.choices = [type("Choice", (), {"text": text})()]


def fake_completion_factory(expected_prompt: str, response_text: str):
    def fake_completion(**kwargs):
        assert kwargs["prompt"] == expected_prompt
        return FakeResponse(response_text)

    return fake_completion


def test_ensure_columns_valid():
    df = pd.DataFrame(columns=["Service Description in English", "AI Category Title"])
    ensure_columns(df, ["Service Description in English", "AI Category Title"])


def test_ensure_columns_missing():
    df = pd.DataFrame(columns=["Service Description in English"])
    try:
        ensure_columns(df, ["Service Description in English", "AI Category Title"])
    except ValueError as exc:
        assert "AI Category Title" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing columns")


def test_classifier_returns_title():
    service_description = "Supply of network cabling services."
    prompt = (
        "You are a UNSPSC classifier. Return ONLY the UNSPSC category title that best matches "
        "the service description. If uncertain, choose the closest relevant title. "
        f"Service description: '{service_description}'."
    )
    fake_client = OpenAICompletionClient(
        create_completion=fake_completion_factory(prompt, "Network cabling services")
    )
    classifier = UnspscClassifier("fake-key", fake_client)
    assert classifier.classify(service_description) == "Network cabling services"


def test_classifier_handles_empty_description():
    fake_client = OpenAICompletionClient(create_completion=lambda **_: FakeResponse(""))
    classifier = UnspscClassifier("fake-key", fake_client)
    assert classifier.classify("  ") == "No UNSPSC Found"


def test_process_batches_writes_output(tmp_path):
    df = pd.DataFrame(
        {
            "Service Description in English": ["Office cleaning", "IT support"],
            "AI Category Title": ["", ""],
        }
    )
    output_path = tmp_path / "output.csv"

    def fake_completion(**_):
        return FakeResponse("Facility maintenance services")

    classifier = UnspscClassifier(
        "fake-key", OpenAICompletionClient(create_completion=fake_completion)
    )
    process_batches(df, classifier, str(output_path), batch_size=1)

    output_df = pd.read_csv(output_path)
    assert output_df["AI Category Title"].tolist() == [
        "Facility maintenance services",
        "Facility maintenance services",
    ]
