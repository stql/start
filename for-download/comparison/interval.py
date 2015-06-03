import operator
from numbers import Number

def get_avg(a, b): return (a + b) / 2
def get_left(a, b): return a
def get_right(a, b): return b

vd_model_dict = {
    'vd_sum': operator.add,
    'vd_diff': operator.sub,
    'vd_product': operator.mul,
    'vd_quotient': operator.truediv,
    'vd_avg': get_avg,
    'vd_max': max,
    'vd_min': min,
    'vd_left': get_left,
    'vd_right': get_right
}

class Interval:
    def __init__(self, start, end, value=None):
        self.start = int(start)
        self.end = int(end)
        if value is not None:
            self.value = float(value)
        else:
            self.value = -1
    
    def overlaps(self, start, end=None):
        if end is not None:
            return self.start <= end and self.end >= start
        elif isinstance(start, Number):
            return self.contains_point(start)
        else:   # duck-typed interval
            return self.overlaps(start.start, start.end)

    def precedes(self, other):
        return self.end < other.start

    def follows(self, other):
        return self.start > other.end

    def contains(self, other):
        return self.start <= other.start and self.end >= other.end
    
    def contains_point(self, p):
        return self.start <= p < self.end
    
    def matches(self, other):
        return self.start == other.start and self.end == other.end
    
    def contains_interval(self, other):
        return self.start <= other.start and self.end >= other.end

    def adjacent(self, other):
        return self.end + 1 == other.start or self.start - 1 == other.end

    def gen_overlap(self, other, vd_model='vd_left'):
        """
        This serves intersectjoin, to compute the final value. It contains 9 kinds of vd_model, and we provide a default
        in case user doesn't pass this argument. For discretization, we only have 5 value models, and this function can
        also handle the latter.
        @param other:
        @type other:
        @param vd_model:
        @type vd_model:
        @return:
        @rtype:
        """
        if not self.overlaps(other):
            raise Exception('The two intervals doesn\'t overlap')
        value = vd_model_dict[vd_model](self.value, other.value)
        return Interval(max(self.start, other.start), min(self.end, other.end), value)

    def distance_to(self, other):
        """
        Returns the size of the gap between intervals, or 0 
        if they touch or overlap.
        """
        if self.overlaps(other):
            return 0
        elif self.end < other.start:
            return other.start - self.end
        elif self.start > other.end:
            return self.start - other.end
        else:
            raise Exception('Please check your Interval')

    def to_tuple(self):
        return self.start, self.end, self.value

    def to_chr_string(self, chr, fields_number=4):
        if fields_number == 4:
            return str(chr) + '\t' + str(self.start) + '\t' + str(self.end) + '\t' + str(self.value)
        elif fields_number == 3:
            return str(chr) + '\t' + str(self.start) + '\t' + str(self.end)
    
    def __len__(self):
        return self.end - self.start + 1
    
    def __hash__(self):
        return hash((self.start, self.end))
    
    def __str__(self):
        return str(self.__unicode__())
    
    def __unicode__(self):
        fields = map(repr, [self.start, self.end, self.value])
        return "Interval({}, {}, {})".format(*fields)
    
    def __repr__(self):
        fields = map(repr, [self.start, self.end, self.value])
        return "Interval({}, {}, {})".format(*fields)
    
    def copy(self):
        return Interval(self.start, self.end, self.value)
    
    def __reduce__(self):
        return Interval, (self.start, self.end, self.value)

    def __eq__(self, other):
        if isinstance(other, Interval):
            return self.start == other.start and self.end == other.end
        return NotImplemented

    @staticmethod
    def _make(t):
        assert len(t) == 3
        return Interval(int(t[0]), int(t[1]), float(t[2]))

