from abc import abstractmethod, ABC
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from dataclasses import field, dataclass
from array_api_compat import get_namespace, is_cupy_array, is_numpy_array
import math
import typing
import warnings

import ezmsg.core as ez

try:
    import numpy as np
    import numpy.typing as npt
    import numpy.lib.stride_tricks as nps
except ModuleNotFoundError:
    ez.logger.error(
        'Install ezmsg with the AxisArray extra:pip install "ezmsg[AxisArray]"'
    )
    raise

from ezmsg.core.util import either_dict_or_kwargs
from .util import replace


if typing.TYPE_CHECKING:
    try:
        from xarray import DataArray
    except ImportError:
        pass

# TODO: Typehinting needs continued help
#  concatenate/transpose should probably not be staticmethods


@dataclass
class AxisBase(ABC):
    """Abstract base class for axes types used by AxisArray."""
    unit: str = ""

    @typing.overload
    def value(self, x: int) -> typing.Any: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray: ...
    @abstractmethod
    def value(self, x):
        raise NotImplementedError


@dataclass
class LinearAxis(AxisBase):
    """
    An axis implementation for sparse axes with regular intervals between elements. 
    
    It is called "linear" because it provides a simple linear mapping between
    indices and element values: value = (index * gain) + offset
    A typical example is a time axis, with regular sampling rate. 
    
    :param gain: Step size (scaling factor) for the linear axis
    :type gain: float
    :param offset: The offset (value) of the first sample
    :type offset: float
    :param unit: The unit of measurement for this axis (inherited from AxisBase)
    :type unit: str
    """
    gain: float = 1.0
    offset: float = 0.0

    @typing.overload
    def value(self, x: int) -> float: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray[np.float64]: ...
    def value(self, x):
        """
        Convert index(es) to axis value(s) using the linear transformation.
        
        :param x: Index or array of indices to convert
        :type x: int | npt.NDArray[np.int_]
        :return: Corresponding axis value(s)
        :rtype: float | npt.NDArray[np.float64]
        """
        return (x * self.gain) + self.offset

    @typing.overload
    def index(self, v: float, fn: Callable = np.rint) -> int: ...
    @typing.overload
    def index(
        self, v: npt.NDArray[np.float64], fn: Callable = np.rint
    ) -> npt.NDArray[np.int_]: ...
    def index(self, v, fn=np.rint):
        """
        Convert axis value(s) to index(es) using the inverse linear transformation.

        :param v: Axis value or array of values to convert to indices
        :type v: float | npt.NDArray[np.float64]
        :param fn: Function to apply for rounding (default: np.rint)
        :type fn: collections.abc.Callable
        :return: Corresponding index or array of indices
        :rtype: int | npt.NDArray[np.int_]
        """
        return fn((v - self.offset) / self.gain).astype(int)

    @classmethod
    def create_time_axis(cls, fs: float, offset: float = 0.0) -> "LinearAxis":
        """
        Convenience method to construct a LinearAxis for time.
        
        :param fs: Sampling frequency in Hz
        :type fs: float
        :param offset: Time offset in seconds (default: 0.0)
        :type offset: float
        :return: A LinearAxis configured for time with gain=1/fs and unit='s'
        :rtype: LinearAxis
        """
        return cls(unit="s", gain=1.0 / fs, offset=offset)


@dataclass
class ArrayWithNamedDims:
    """
    Base class for arrays with named dimensions.
    
    This class provides a foundation for arrays where each dimension has a name,
    enabling more intuitive array manipulation and access patterns.
    
    :param data: The underlying numpy array data
    :type data: npt.NDArray
    :param dims: List of dimension names, must match the array's number of dimensions
    :type dims: list[str]
    
    :raises ValueError: If dims length doesn't match data.ndim or contains duplicate names
    """
    data: npt.NDArray
    dims: list[str]

    def __post_init__(self):
        if len(self.dims) != self.data.ndim:
            raise ValueError("dims must be same length as data.shape")
        if len(self.dims) != len(set(self.dims)):
            raise ValueError("dims contains repeated dim names")

    def __eq__(self, other):
        if self is other:
            return True
        if other.__class__ is self.__class__:
            xp = get_namespace(self.data)
            if self.dims == other.dims and xp.array_equal(self.data, other.data):
                return True
        return NotImplemented


@dataclass(eq=False)
class CoordinateAxis(AxisBase, ArrayWithNamedDims):
    """
    An axis implementation that uses explicit coordinate values stored in an array.
    
    This class allows for non-linear or irregularly spaced coordinate systems
    by storing the actual coordinate values in a data array.
    
    Inherits from both AxisBase and ArrayWithNamedDims, combining axis functionality
    with named dimension support.

    :param unit: The unit of measurement for this axis (inherited from AxisBase)
    :type unit: str
    :param data: The underlying numpy array data (inherited from ArrayWithNamedDims)
    :type data: npt.NDArray
    :param dims: List of dimension names, must match the array's number of dimensions (inherited from ArrayWithNamedDims)
    :type dims: list[str]
    """
    @typing.overload
    def value(self, x: int) -> typing.Any: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray: ...
    def value(self, x):
        """
        Get coordinate value(s) at the given index(es).
        
        :param x: Index or array of indices to lookup
        :type x: int | npt.NDArray[np.int_]
        :return: Coordinate value(s) at the specified index(es)
        :rtype: typing.Any | npt.NDArray
        """
        return self.data[x]


@dataclass(eq=False)
class AxisArray(ArrayWithNamedDims):
    """
    A lightweight message class comprising a numpy ndarray and its metadata.
    
    AxisArray extends ArrayWithNamedDims to provide a complete data structure for
    scientific computing with named dimensions, axis coordinate systems, and metadata.
    It's designed to be similar to xarray.DataArray but optimized for message passing
    in streaming applications.
    
    :param data: The underlying numpy array data (inherited from ArrayWithNamedDims)
    :type data: npt.NDArray
    :param dims: List of dimension names (inherited from ArrayWithNamedDims)  
    :type dims: list[str]
    :param axes: Dictionary mapping dimension names to their axis coordinate systems
    :type axes: dict[str, AxisBase]
    :param attrs: Dictionary of arbitrary metadata attributes
    :type attrs: dict[str, typing.Any]
    :param key: Optional key identifier for this array, typically used to specify source device (default is empty string)
    :type key: str
    """
    axes: dict[str, AxisBase] = field(default_factory=dict)
    attrs: dict[str, typing.Any] = field(default_factory=dict)
    key: str = ""

    T = typing.TypeVar("T", bound="AxisArray")

    def __eq__(self, other):
        # NOTE: ArrayWithNamedDims __eq__ checks for __class__ equivalence
        # returns NotImplemented if classes aren't equal.  Unintuitively,
        # NotImplemented seems to evaluate as 'True' in an if statement.
        equal = super().__eq__(other)
        if equal != True:
            return equal

        # checks for AxisArray fields
        if (
            self.key == other.key
            and self.attrs == other.attrs
            and self.axes == other.axes
        ):
            return True

        return False

    @dataclass
    class Axis(LinearAxis):
        # deprecated backward compatibility
        def __post_init__(self) -> None:
            warnings.warn(
                "AxisArray.Axis is a deprecated alias for LinearAxis",
                DeprecationWarning,
                stacklevel=2,
            )

        @classmethod
        def TimeAxis(cls, fs: float, offset: float = 0.0) -> "AxisArray.Axis":
            return cls(unit="s", gain=1.0 / fs, offset=offset)

        def units(self, idx):
            # NOTE: This is poorly named anyway, it should have been `value`
            return (idx * self.gain) + self.offset

        def index(self, val):
            return np.round((val - self.offset) / self.gain)

    TimeAxis = LinearAxis.create_time_axis
    LinearAxis = LinearAxis
    CoordinateAxis = CoordinateAxis

    @dataclass(frozen=True)
    class AxisInfo:
        """
        Container for axis information including the axis object, index, and size.
        
        This class provides a convenient way to access both the axis coordinate system
        and metadata about where that axis appears in the array structure.
        
        :param axis: The axis coordinate system
        :type axis: AxisBase
        :param idx: The index of this axis in the AxisArray object's dimension list
        :type idx: int  
        :param size: The size of this dimension (stored as None for CoordinateAxis which determines size from data)
        :type size: int | None
        
        :raises ValueError: If size rules are violated for the axis type
        """
        axis: AxisBase
        idx: int
        # TODO (kpilch): rename this to _size as preferred usage is len(obj), not obj.size
        size: int | None = None

        def __post_init__(self) -> None:
            if not isinstance(self.axis, CoordinateAxis) and self.size is None:
                raise ValueError("must define size if not using CoordinateAxis")
            elif isinstance(self.axis, CoordinateAxis) and self.size is not None:
                raise ValueError("must not define size if using CoordinateAxis")

        def __len__(self) -> int:
            if self.size is None:
                if isinstance(self.axis, CoordinateAxis):
                    return self.axis.data.size
                else:
                    raise ValueError
            else:
                return self.size

        @property
        def indices(self) -> npt.NDArray[np.int_]:
            """
            Get array of all valid indices for this axis.
            
            :return: Array of indices from 0 to len(self)-1
            :rtype: npt.NDArray[np.int_]
            """
            return np.arange(len(self))

        @property
        def values(self) -> npt.NDArray:
            """
            Get array of coordinate values for all indices of this axis.
            
            :return: Array of coordinate values computed from axis.value(indices)
            :rtype: npt.NDArray
            """
            return self.axis.value(self.indices)

    def isel(
        self: T,
        indexers: typing.Any | None = None,
        **indexers_kwargs: typing.Any,
    ) -> T:
        """
        Select data using integer-based indexing along specified dimensions.
        
        This method allows for flexible indexing using integers, slices, or arrays
        of integers to select subsets of the data along named dimensions.
        
        :param indexers: Dictionary of {dimension_name: indexer} pairs
        :type indexers: typing.Any | None
        :param indexers_kwargs: Alternative way to specify indexers as keyword arguments  
        :type indexers_kwargs: typing.Any
        :return: New AxisArray with selected data
        :rtype: T
        :raises ValueError: If indexer types are not supported (arrays, ints, or slices only)
        """
        xp = get_namespace(self.data)

        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "isel")

        out_axes = {an: a for an, a in self.axes.items()}
        out_data = self.data

        for axis_name, ix in indexers.items():
            if not isinstance(ix, (np.ndarray, int, slice)):
                raise ValueError("isel only accepts arrays, ints, or slices")
            ax = self.ax(axis_name)
            indices = xp.arange(len(ax))

            # There has to be a more efficient way to do this
            if isinstance(ix, slice):
                indices = indices[ix]
            else:
                ix = [ix] if isinstance(ix, int) else ix

                indices = xp.take(indices, ix, 0)
            if axis_name in out_axes:
                out_axes[axis_name] = replace(ax.axis, offset=ax.axis.value(indices[0]))
            out_data = xp.take(out_data, indices, ax.idx)

        return replace(self, data=out_data, axes=out_axes)

    def sel(
        self: T,
        indexers: typing.Any | None = None,
        **indexers_kwargs: typing.Any,
    ) -> T:
        """
        Select data using label-based indexing along specified dimensions.
        
        This method allows selection using real-world coordinate values rather than
        integer indices. Currently supports only slice objects and LinearAxis.
        
        :param indexers: Dictionary of {dimension_name: slice_indexer} pairs
        :type indexers: typing.Any | None
        :param indexers_kwargs: Alternative way to specify indexers as keyword arguments
        :type indexers_kwargs: typing.Any  
        :return: New AxisArray with selected data
        :rtype: T
        :raises ValueError: If indexer is not a slice or axis is not a LinearAxis
        """
        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "sel")

        out_indexers = dict()
        for axis_name, ix in indexers.items():
            axis = self.get_axis(axis_name)
            if not isinstance(ix, slice):
                raise ValueError("sel only supports slices for now")
            if not isinstance(axis, LinearAxis):
                raise ValueError("sel only supports LinearAxis for now")
            start = int(axis.index(ix.start)) if ix.start is not None else None
            stop = int(axis.index(ix.stop)) if ix.stop is not None else None
            step = int(ix.step / axis.gain) if ix.step is not None else None
            out_indexers[axis_name] = slice(start, stop, step)

        return self.isel(**out_indexers)

    @property
    def shape(self) -> tuple[int, ...]:
        """
        Shape of data.
        
        :return: Tuple representing the shape of the underlying data array
        :rtype: tuple[int, ...]
        """
        return self.data.shape

    def to_xr_dataarray(self) -> "DataArray":
        """
        Convert this AxisArray to an xarray DataArray.
        
        This method creates an xarray DataArray with equivalent data, coordinates,
        dimensions, and attributes. Useful for interoperability with the xarray ecosystem.
        
        :return: xarray DataArray representation of this AxisArray
        :rtype: xarray.DataArray
        """
        from xarray import DataArray

        coords = {}
        for name, axis in self.axes.items():
            if name in self.dims:
                coords[name] = (name, self.ax(name).values)
            elif isinstance(axis, CoordinateAxis):
                coords[name] = (axis.dims, axis.data)

        return DataArray(self.data, coords=coords, dims=self.dims, attrs=self.attrs)

    def ax(self, dim: str | int) -> AxisInfo:
        """
        Get AxisInfo for a specified dimension.
        
        :param dim: Dimension name or index
        :type dim: str | int
        :return: AxisInfo containing axis, index, and size information
        :rtype: AxisInfo
        """
        axis_idx = dim if isinstance(dim, int) else self.get_axis_idx(dim)
        axis_name = dim if isinstance(dim, str) else self.get_axis_name(dim)
        return AxisArray.AxisInfo(
            axis=self.get_axis(axis_name), idx=axis_idx, size=self.shape[axis_idx]
        )

    def get_axis(self, dim: str | int) -> AxisBase:
        """
        Get the axis coordinate system for a specified dimension.
        
        :param dim: Dimension name or index
        :type dim: str | int
        :return: The axis coordinate system (defaults to LinearAxis if not specified)
        :rtype: AxisBase
        :raises ValueError: If dimension is not present in the object
        """
        if isinstance(dim, int):
            dim = self.get_axis_name(dim)
        if dim not in self.dims:
            raise ValueError(f"{dim=} not present in object")
        return self.axes.get(dim, LinearAxis())  # backward compatible

    def get_axis_name(self, dim: int) -> str:
        """
        Get the dimension name for a given axis index.
        
        :param dim: The axis index
        :type dim: int
        :return: The dimension name
        :rtype: str
        """
        return self.dims[dim]

    def get_axis_idx(self, dim: str) -> int:
        """
        Get the axis index for a given dimension name.
        
        :param dim: The dimension name
        :type dim: str
        :return: The axis index
        :rtype: int
        :raises ValueError: If dimension is not present in the object
        """
        try:
            return self.dims.index(dim)
        except ValueError:
            raise ValueError(f"{dim=} not present in object")

    def axis_idx(self, dim: str | int) -> int:
        """
        Get the axis index for a given dimension name or pass through if already an int.
        
        :param dim: Dimension name or index
        :type dim: str | int
        :return: The axis index
        :rtype: int
        """
        return self.get_axis_idx(dim) if isinstance(dim, str) else dim

    def _axis_idx(self, dim: str | int) -> int:
        """Deprecated; use axis_idx instead"""
        return self.axis_idx(dim)

    def as2d(self, dim: str | int) -> npt.NDArray:
        """
        Get a 2D view of the data with the specified dimension as the first axis.
        
        :param dim: Dimension name or index to move to first axis
        :type dim: str | int
        :return: 2D array view with shape (dim_size, remaining_elements)
        :rtype: npt.NDArray
        """
        return as2d(self.data, self.axis_idx(dim), xp=get_namespace(self.data))

    def iter_over_axis(
        self: T, axis: str | int
    ) -> Generator[T, None, None]:
        """
        Iterate over slices along the specified axis.
        
        Yields AxisArray objects for each slice along the given axis, with that
        dimension removed from the resulting arrays.
        
        :param axis: Dimension name or index to iterate over
        :type axis: str | int
        :yields: AxisArray objects for each slice along the axis
        :rtype: collections.abc.Generator[T, None, None]
        """
        xp = get_namespace(self.data)
        axis_idx = self.axis_idx(axis)
        dim_name = self.dims[axis_idx]
        new_dims = [d for i, d in enumerate(self.dims) if i != axis_idx]
        new_axes = {d: a for d, a in self.axes.items() if d != dim_name}

        for it_data in xp.moveaxis(self.data, axis_idx, 0):
            it_aa = replace(
                self,
                data=it_data,
                dims=new_dims,
                axes=new_axes,
            )
            yield it_aa

    @contextmanager
    def view2d(
        self, dim: str | int
    ) -> Generator[npt.NDArray, None, None]:
        """
        Context manager providing a 2D view of the data.
        
        Yields a 2D array view with the specified dimension as the first axis.
        Changes to the yielded array may be reflected in the original data.
        
        :param dim: Dimension name or index to move to first axis  
        :type dim: str | int
        :yields: 2D array view with shape (dim_size, remaining_elements)
        :rtype: collections.abc.Generator[npt.NDArray, None, None]
        """
        with view2d(self.data, self.axis_idx(dim)) as arr:
            yield arr

    def shape2d(self, dim: str | int) -> tuple[int, int]:
        """
        Get the 2D shape when viewing data with specified dimension first.
        
        :param dim: Dimension name or index  
        :type dim: str | int
        :return: Tuple of (dim_size, remaining_elements)
        :rtype: tuple[int, int]
        """
        return shape2d(self.data, self.axis_idx(dim))

    @staticmethod
    def concatenate(
        *aas: T,
        dim: str,
        axis: AxisBase | None = None,
        filter_key: str | None = None,
    ) -> T:
        """
        Concatenate multiple AxisArray objects along a specified dimension.
        
        :param aas: Variable number of AxisArray objects to concatenate
        :type aas: T
        :param dim: Dimension name along which to concatenate
        :type dim: str
        :param axis: Optional axis coordinate system for the concatenated dimension
        :type axis: AxisBase | None
        :param filter_key: Optional key filter - only concatenate arrays with matching key
        :type filter_key: str | None
        :return: New AxisArray with concatenated data
        :rtype: T
        :raises ValueError: If objects have incompatible dimensions
        """
        xp = get_namespace(*[aa.data for aa in aas])
        if filter_key is not None:
            aas = tuple([aa for aa in aas if aa.key == filter_key])

        aa_0 = aas[0]
        for aa in aas[1:]:
            if aa.dims != aa_0.dims:
                raise ValueError("objects have incompatible dims")

        new_dims = [d for d in aa_0.dims]
        new_axes = {ax_name: ax for ax_name, ax in aa_0.axes.items()}
        new_key = aa_0.key if all(aa.key == aa_0.key for aa in aas) else ""

        if axis is not None:
            new_axes[dim] = axis
        elif all((dim in aa.axes and hasattr(aa.axes[dim], "data") for aa in aas)):
            ax_xp = get_namespace(aa_0.axes[dim].data)
            new_axes[dim] = CoordinateAxis(
                data=ax_xp.concatenate([aa.axes[dim].data for aa in aas]),
                dims=aa_0.axes[dim].dims,
                unit=aa_0.axes[dim].unit,
            )
        # else LinearAxis -> no change needed, just use the first one

        all_data = [aa.data for aa in aas]

        if dim in aa_0.dims:
            dim_idx = aa_0.axis_idx(dim=dim)
            new_data = xp.concatenate(all_data, axis=dim_idx)
            return replace(
                aa_0, data=new_data, dims=new_dims, axes=new_axes, key=new_key
            )

        else:
            new_data = xp.array(all_data)
            new_dims = [dim] + new_dims
            return replace(
                aa_0, data=new_data, dims=new_dims, axes=new_axes, key=new_key
            )

    @staticmethod
    def transpose(
        aa: T,
        dims: Iterable[str] | Iterable[int] | None = None,
    ) -> T:
        """
        Transpose (reorder) the dimensions of an AxisArray.
        
        :param aa: The AxisArray to transpose
        :type aa: T
        :param dims: New dimension order (names or indices). If None, reverses all dimensions
        :type dims: collections.abc.Iterable[str] | collections.abc.Iterable[int] | None
        :return: New AxisArray with transposed dimensions
        :rtype: T
        """
        xp = get_namespace(aa.data)
        dims = reversed(range(aa.data.ndim)) if dims is None else dims
        dim_indices = [aa.axis_idx(d) for d in dims]
        new_dims = [aa.dims[d] for d in dim_indices]
        new_data = xp.transpose(aa.data, dim_indices)
        return replace(aa, data=new_data, dims=new_dims, axes=aa.axes)


def slice_along_axis(
    in_arr: npt.NDArray, sl: slice | int, axis: int
) -> npt.NDArray:
    """
    Slice the input array along a specified axis using the given slice object or integer index.
     Integer arguments to `sl` will cause the sliced dimension to be dropped.
     Use `slice(my_int, my_int+1, None)` to keep the sliced dimension.

    :param in_arr: The input array to be sliced
    :type in_arr: npt.NDArray
    :param sl: The slice object or integer index to use for slicing
    :type sl: slice | int
    :param axis: The axis along which to slice the array
    :type axis: int

    :return: The sliced array (view)
    :rtype: npt.NDArray
    :raises ValueError: If the axis value is invalid for the input array
    """
    if axis < -in_arr.ndim or axis >= in_arr.ndim:
        raise ValueError(
            f"Invalid axis value {axis} for input array with {in_arr.ndim} dimensions."
        )
    if -in_arr.ndim <= axis < 0:
        axis = in_arr.ndim + axis
    all_slice = (
        (slice(None),) * axis + (sl,) + (slice(None),) * (in_arr.ndim - axis - 1)
    )
    return in_arr[all_slice]


def sliding_win_oneaxis(
    in_arr: npt.NDArray, nwin: int, axis: int, step: int = 1
) -> npt.NDArray:
    """
    Generates a view of an array using a sliding window of specified length along a specified axis of the input array.
    This is a slightly optimized version of nps.sliding_window_view with a few important differences:

    - This only accepts a single nwin and a single axis, thus we can skip some checks.
    - The new `win` axis precedes immediately the original target axis, unlike sliding_window_view where the
        target axis is moved to the end of the output.

    Combine this with slice_along_axis(..., sl=slice(None, None, step), axis=axis) to step the window
        by more than 1 sample at a time.

    :param in_arr: The input array
    :type in_arr: npt.NDArray
    :param nwin: The size of the sliding window
    :type nwin: int
    :param axis: The axis along which the sliding window will be applied
    :type axis: int
    :param step: The size of the step between windows. If > 1, the strided window will be sliced with `slice_along_axis` (default = 1)
    :type step: int

    :return: A view to the input array with the sliding window applied
    :rtype: npt.NDArray

    Note: There is a known edge case when nwin == shape[axis] + 1. While this should raise
        an error because the window is larger than the input, the implementation ends up
        returning a 0-length window. We could check for this but this function is intended
        to have minimal latency so we have decided to skip the checks and deal with the
        support issues as they arise.
    """
    if -in_arr.ndim <= axis < 0:
        axis = in_arr.ndim + axis

    is_f_order = in_arr.flags.f_contiguous and not in_arr.flags.c_contiguous

    out_shape = (
        in_arr.shape[:axis]
        + (in_arr.shape[axis] - (nwin - 1),)
        + (nwin,)
        + in_arr.shape[axis + 1 :]
    )

    stride_axis = in_arr.strides[axis]
    out_strides = (
        in_arr.strides[:axis]
        + ((stride_axis,) * 2 if not is_f_order else (stride_axis,) * 2)
        + in_arr.strides[axis + 1 :]
    )

    if is_numpy_array(in_arr):
        from numpy.lib.stride_tricks import as_strided  # type: ignore
        from functools import partial

        as_strided = partial(as_strided, writeable=False)
    elif is_cupy_array(in_arr):
        from cupy.lib.stride_tricks import as_strided  # type: ignore
    else:
        raise Exception(
            f"Unsupported array module for sliding_win_oneaxis: {get_namespace(in_arr)}"
        )
    result = as_strided(in_arr, strides=out_strides, shape=out_shape)
    if step > 1:
        result = slice_along_axis(result, slice(None, None, step), axis)
    return result


def _as2d(
    in_arr: npt.NDArray, axis: int = 0, *, xp
) -> tuple[npt.NDArray, tuple[int, ...]]:
    """
    Internal helper function to reshape array to 2D with specified axis first.
    
    :param in_arr: Input array to be reshaped
    :type in_arr: npt.NDArray
    :param axis: Axis to move to first position (default: 0)
    :type axis: int
    :param xp: Array namespace (numpy, cupy, etc.)
    :return: Tuple of (reshaped_2d_array, original_remaining_shape)
    :rtype: tuple[npt.NDArray, tuple[int, ...]]
    """
    arr = in_arr
    if arr.ndim == 0:
        arr = arr.reshape(1, 1)
    elif arr.ndim == 1:
        arr = arr[:, np.newaxis]

    arr = xp.moveaxis(arr, axis, 0)
    remaining_shape = arr.shape[1:]
    arr = arr.reshape(arr.shape[0], math.prod(remaining_shape))
    return arr, remaining_shape


def as2d(in_arr: npt.NDArray, axis: int = 0, *, xp) -> npt.NDArray:
    """
    Reshape array to 2D with specified axis first.
    
    :param in_arr: Input array
    :type in_arr: npt.NDArray
    :param axis: Axis to move to first position (default: 0)
    :type axis: int
    :param xp: Array namespace (numpy, cupy, etc.)
    :return: 2D reshaped array
    :rtype: npt.NDArray
    """
    arr, _ = _as2d(in_arr, axis=axis, xp=xp)
    return arr


@contextmanager
def view2d(
    in_arr: npt.NDArray, axis: int = 0
) -> Generator[npt.NDArray, None, None]:
    """
    Context manager providing 2D view of the array, no matter what input dimensionality is.
    Yields a view of underlying data when possible, changes to data in yielded
    array will always be reflected in self.data, either via the view or copying.

    NOTE: In practice, I'm not sure this is very useful because it requires modifying
    the numpy array data in-place, which limits its application to zero-copy messages

    NOTE: The context manager allows the use of `with` when calling `view2d`. 
    
    :param in_arr: Input array to be viewed as 2D
    :type in_arr: npt.NDArray
    :param axis: Dimension index to move to first axis (Default = 0)
    :type axis: int
    :yields: 2D array view with shape (dim_size, remaining_elements)
    :rtype: collections.abc.Generator[npt.NDArray, None, None]
    """
    xp = get_namespace(in_arr)
    arr, remaining_shape = _as2d(in_arr, axis=axis, xp=xp)

    yield arr

    if not np.shares_memory(arr, in_arr):
        arr = arr.reshape(arr.shape[0], *remaining_shape)
        arr = xp.moveaxis(arr, 0, axis)
        in_arr[:] = arr


def shape2d(arr: npt.NDArray, axis: int = 0) -> tuple[int, int]:
    """
    Calculate the 2D shape when viewing array with specified axis first.
    
    :param arr: Input array
    :type arr: npt.NDArray  
    :param axis: Axis to move to first position (default: 0)
    :type axis: int
    :return: Tuple of (axis_size, remaining_elements)
    :rtype: tuple[int, int]
    """
    return (
        arr.shape[axis],
        math.prod(v for idx, v in enumerate(arr.shape) if idx != axis),
    )
