"""
Copyright (c) 2009, Ask Solem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

Neither the name of Ask Solem nor the names of its contributors may be used
to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

"""


class EqualityComparableUsingAttributeDictionary(object):
    """
    Instances of classes inheriting from this class can be compared
    using their attribute dictionary (__dict__). See GitHub issue
    128 and http://stackoverflow.com/q/390640
    """

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self == other


class ESRange(EqualityComparableUsingAttributeDictionary):
    """
    From py.es
    """

    def __init__(
        self,
        field,
        from_value=None,
        to_value=None,
        include_lower=None,
        include_upper=None,
        **kwargs
    ):
        self.field = field
        self.from_value = from_value
        self.to_value = to_value
        self.include_lower = include_lower
        self.include_upper = include_upper

    def negate(self):
        """Reverse the range"""
        self.from_value, self.to_value = self.to_value, self.from_value
        self.include_lower, self.include_upper = self.include_upper, self.include_lower

    def serialize(self):
        filters = {}
        if self.from_value is not None:
            include_lower = "gte" if self.include_lower else "gt"
            filters[include_lower] = self.from_value

        if self.to_value is not None:
            include_upper = "lte" if self.include_upper else "lt"
            filters[include_upper] = self.to_value

        return self.field, filters


class ESRangeOp(ESRange):
    def __init__(self, field, op1, value1, op2=None, value2=None):
        from_value = to_value = include_lower = include_upper = None
        for op, value in ((op1, value1), (op2, value2)):
            if op == "gt":
                from_value = value
                include_lower = False
            elif op == "gte":
                from_value = value
                include_lower = True
            if op == "lt":
                to_value = value
                include_upper = False
            elif op == "lte":
                to_value = value
                include_upper = True
        super(ESRangeOp, self).__init__(field, from_value, to_value, include_lower, include_upper)
