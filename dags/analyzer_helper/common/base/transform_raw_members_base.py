class TransformRawMembersBase:
    def transform(self) -> list[dict]:
        """
        transform members data to the given structure
        """
        raise NotImplementedError("This method should be overridden by subclasses")