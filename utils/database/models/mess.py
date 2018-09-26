class SourceConstructor:
    def __init__(self, **kwargs):
        """
            Helper class to parse and fetch data_source, source_code, data_source_code;

            # source_code = config.SourceConstructor(source_code=1, data_source_code=3)
            # source_code = config.SourceConstructor(data_source="010003")

        Args:
            **kwargs:
            source_code: int
            data_source_code: int
            data_source: str
        """
        self._source_code, self._data_source_code = kwargs.get("source_code"), kwargs.get("data_source_code")
        self._data_source = kwargs.get("data_source")
        self.max_source_code_length, self.max_data_source_code_length = 2, 4

    @property
    def source_code(self):
        if self._source_code is not None:
            return str(self._source_code)
        else:
            return int(self.data_source[:2])

    @property
    def data_source_code(self):
        if self._data_source_code is not None:
            return str(self._data_source_code)
        else:
            return int(self.data_source[2:6])

    def _construct_data_source(self):
        if self._data_source is None:
            data_source = "{pad_s}{s_code}{pad_ds}{ds_code}".format(
                pad_s=(self.max_source_code_length - len(self.source_code)) * "0",
                s_code=self.source_code,
                pad_ds=(self.max_data_source_code_length - len(self.data_source_code)) * "0",
                ds_code=self.data_source_code
            )
            self._data_source = data_source
        return self._data_source

    @property
    def data_source(self):
        if self._data_source is None:
            return self._construct_data_source()
        else:
            return self._data_source
