from openai import OpenAI


class Classifier:
    def __init__(self) -> None:
        self.client = OpenAI()

        # configurations
        self.model = "ft:gpt-3.5-turbo-0125:togethercrew::9mHnDhmo"
        self.system_content = "You are a classifier with a PhD in psychology and 30 years of experience teaching non-violent communication. "
        self.user_context = (
            "[INSTRUCTION]\n"
            "State whether the message below contains any of the following violations: Discriminating, Identifying, Sexualized and/or Toxic. If not, provide None as answer.\n\n"
            "[MESSAGE]\n"
        )

    def classify(self, text: str) -> str:
        """
        classify the given text

        Parameters
        -----------
        text : str
            the text to be classified

        Returns
        ---------
        label : str
            the text label
        """
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": self.system_content,
                },
                {
                    "role": "user",
                    "content": self.user_context
                    + text
                    + "\n\n[VIOLATION DETECTIONS]\n",
                },
            ],
            n=1,
        )

        # the label to add to general data structure
        label = completion.choices[0].message.content

        return label
