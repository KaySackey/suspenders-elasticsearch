class Result:
    def __init__(self, doc):
        doc = doc.copy()

        self._id = None
        self._attributes = []

        for field_name in ["_source", "fields"]:
            # Aggregate fields from different sources
            if field_name in doc:
                for key, value in doc[field_name].items():
                    setattr(self, key, value)
                    self._attributes.append(key)

                del doc[field_name]

        for key, value in doc.items():
            setattr(self, key, value)
            self._attributes.append(key)

        # Add id back in
        self.id = self._id
        self._attributes.append("id")

    def as_dict(self):
        doc = {}
        for key in self._attributes:
            # Keys are strings so the dictionary can easily be used as a **kwarg
            doc[str(key)] = getattr(self, key)

        return doc

    def __str__(self):
        return str(self.as_dict())

    def __repr__(self):
        return str(self)
