class LoadTransformedMembersBase:
	def __init__(self, platform_id: str):
		"""
		initialize the load class for a specific platform
		"""
		self._platform_id = platform_id

	def get_platform_id(self) -> str:
		"""
		returns the platform ID for subclasses
		"""
		return self._platform_id

	def load(self, processed_data: list[dict], recompute: bool = False):
		"""
		load the members transformed data into their
		respective collection under platform_id database
		if recompute is True, then replace the whole data with the processed data
		"""
		pass
