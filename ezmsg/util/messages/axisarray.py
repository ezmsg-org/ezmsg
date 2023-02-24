import math
import time

from contextlib import contextmanager
from dataclasses import field, dataclass, replace

import numpy as np
import numpy.typing as npt

from typing import Generator, Tuple, Optional, List, Dict, Union, Iterable

@dataclass
class AxisArray:
    data: npt.NDArray
    dims: List[str]
    axes: Dict[str, "AxisArray.Axis"] = field(default_factory = dict)

    @dataclass
    class Axis:
        unit: str = ''
        gain: float = 1.0
        offset: float = 0.0

        @classmethod
        def TimeAxis(cls, fs: float, offset: float = 0.0) -> "AxisArray.Axis":
            """ 
            Creates a time axis with dimension of seconds. 
            Specify fs in units of Hz (1.0/sec)
            NOTE: offset corresponds to idx[0] on this dimension!
            """
            return cls(unit = 's', gain = 1.0 / fs, offset = time.time() if offset is None else offset)

    def __post_init__(self):
        self.data = np.array(self.data, ndmin=1)

        if len(self.dims) != self.data.ndim:
            raise ValueError("dims must be same length as data.shape")
        if len(self.dims) != len(set(self.dims)):
            raise ValueError("dims contains repeated dim names")
   
    @property
    def shape(self) -> Tuple[int, ...]:
        """ Shape of data """
        return self.data.shape

    def get_axis(self, dim: Union[str, int]) -> Axis:
        if isinstance(dim, int):
            dim = self.get_axis_name(dim)
        if dim not in self.dims:
            raise ValueError(f'{dim=} not present in object')
        return self.axes.get(dim, AxisArray.Axis())

    def get_axis_name(self, dim: int) -> str:
        return self.dims[dim]
            
    def get_axis_idx(self, dim: str) -> int:
        try:
            return self.dims.index(dim)
        except ValueError:
            raise ValueError(f"{dim=} not present in object")

    def _axis_idx(self, dim: Union[str, int]) -> int:
        return self.get_axis_idx(dim) if isinstance(dim, str) else dim

    def as2d(self, dim: Union[str, int]) -> npt.NDArray:
        return as2d(self.data, self._axis_idx(dim))

    @contextmanager
    def view2d(self, dim: Union[str,int]) -> Generator[npt.NDArray, None, None]:
        with view2d(self.data, self._axis_idx(dim)) as arr:
            yield arr

    def shape2d(self, dim: Union[str,int]) -> Tuple[int, int]:
        return shape2d(self.data, self._axis_idx(dim))

    @staticmethod
    def concatenate( *aas: "AxisArray", dim: str, axis: Optional[Axis] = None) -> "AxisArray":
        aa_0 = aas[0]
        for aa in aas[1:]:
            if aa.dims != aa_0.dims:
                raise ValueError('objects have incompatible dims')

        new_dims = [d for d in aa_0.dims]
        new_axes = {ax_name: ax for ax_name, ax in aa_0.axes.items()}

        if axis is not None:
            new_axes[dim] = axis

        all_data = [aa.data for aa in aas]

        if dim in aa_0.dims:
            dim_idx = aa_0._axis_idx(dim)
            new_data = np.concatenate(all_data, axis = dim_idx)
            return replace(aa_0, data = new_data, dims = new_dims, axes = new_axes)

        else:
            new_data = np.array(all_data)
            new_dims = [dim] + new_dims
            return replace(aa_0, data = new_data, dims = new_dims, axes = new_axes)

    @staticmethod
    def transpose(aa: "AxisArray", dims: Optional[Union[Iterable[str], Iterable[int]]] = None) -> "AxisArray":
        dims = reversed(range(aa.data.ndim)) if dims is None else dims
        dim_indices = [aa._axis_idx(d) for d in dims]
        new_dims = [aa.dims[d] for d in dim_indices]
        new_data = np.transpose(aa.data, dim_indices)
        return replace(aa, data = new_data, dims = new_dims, axes = aa.axes)


def _as2d(in_arr: npt.NDArray, axis: int = 0) -> Tuple[npt.NDArray, Tuple[int]]:
    arr = in_arr
    if arr.ndim == 0:
        arr = arr.reshape(1, 1)
    elif arr.ndim == 1:
        arr = arr[:, np.newaxis]

    arr = np.moveaxis(arr, axis, 0)
    remaining_shape = arr.shape[1:]
    arr = arr.reshape(arr.shape[0], np.prod(remaining_shape))
    return arr, remaining_shape

def as2d(in_arr: npt.NDArray, axis: int = 0) -> npt.NDArray:
    arr, _ = _as2d(in_arr, axis = axis)
    return arr

@contextmanager
def view2d(in_arr: npt.NDArray, axis: int = 0) -> Generator[npt.NDArray, None, None]:
    """ 
    2D-View (el x ch) of the array, no matter what input dimensionality is.
    Yields a view of underlying data when possible, changes to data in yielded 
    array will always be reflected in self.data, either via the view or copying
    NOTE: In practice, I'm not sure this is very useful because it requires modifying
    the numpy array data in-place, which limits its application to zero-copy messages
    """
    arr, remaining_shape = _as2d(in_arr, axis = axis)

    yield arr

    if not np.shares_memory(arr, in_arr):
        arr = arr.reshape(arr.shape[0], *remaining_shape)
        arr = np.moveaxis(arr, 0, axis)
        in_arr[:] = arr

def shape2d(arr: npt.NDArray, axis: int = 0) -> Tuple[int, int]:
    return (
        arr.shape[axis],
        math.prod(v for idx, v in enumerate(arr.shape) if idx != axis)
    )