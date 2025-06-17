from abc import abstractmethod, ABC
from contextlib import contextmanager
from dataclasses import field, dataclass
import math
import typing
import warnings

import ezmsg.core as ez

try:
    import numpy as np
    import numpy.typing as npt
    import numpy.lib.stride_tricks as nps
except ModuleNotFoundError:
    ez.logger.error("Install ezmsg with the AxisArray extra:"
                    'pip install "ezmsg[AxisArray]"')
    raise

from ezmsg.core.util import either_dict_or_kwargs
from .util import replace


if typing.TYPE_CHECKING:
    try:
        from xarray import DataArray
    except ImportError:
        pass

# TypeVar for functions that operate on AxisArray and return an instance of the same type
_AxisArrayType = typing.TypeVar("_AxisArrayType", bound="AxisArray")

@dataclass
class AxisBase(ABC):
    """
    Abstract base class for defining an axis.

    An axis describes the meaning of the values along a dimension of an `AxisArray`.
    It can be used to convert between array indices and physical values.

    Attributes:
        unit: The physical unit of the axis values (e.g., "s" for seconds, "Hz" for Hertz).
    """
    unit: str = ""

    @typing.overload
    def value(self, x: int) -> typing.Any: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray: ...
    @abstractmethod
    def value(self, x):
        """
        Converts an index (or array of indices) to its corresponding physical value(s).

        Args:
            x: An integer index or a NumPy array of integer indices.

        Returns:
            The physical value or NumPy array of physical values corresponding to the input index/indices.
        """
        raise NotImplementedError


@dataclass
class LinearAxis(AxisBase):
    """
    Represents a linear transformation between array indices and physical values.

    The transformation is defined by `value = (index * gain) + offset`.

    Attributes:
        gain: The scaling factor applied to the index.
        offset: The offset added to the scaled index.
        unit: The physical unit of the axis values.
    """
    gain: float = 1.0
    offset: float = 0.0

    @typing.overload
    def value(self, x: int) -> float: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray[np.float64]: ...
    def value(self, x):
        """
        Converts an index (or array of indices) to its corresponding physical value(s)
        using the linear transformation: `(x * gain) + offset`.

        Args:
            x: An integer index or a NumPy array of integer indices.

        Returns:
            The physical value (float) or NumPy array of physical values (np.float64)
            corresponding to the input index/indices.
        """
        return (x * self.gain) + self.offset

    @typing.overload
    def index(self, v: float, fn: typing.Callable = np.rint) -> int: ...
    @typing.overload
    def index(
        self, v: npt.NDArray[np.float64], fn: typing.Callable = np.rint
    ) -> npt.NDArray[np.int_]: ...
    def index(self, v, fn=np.rint):
        """
        Converts a physical value (or array of values) to its corresponding array index/indices.

        The transformation is the inverse of `value`: `index = fn((value - offset) / gain)`.

        Args:
            v: A physical value (float) or a NumPy array of physical values.
            fn: A function to apply after calculating the raw index, typically for rounding
                (e.g., `np.rint`, `np.floor`, `np.ceil`). Defaults to `np.rint`.

        Returns:
            The integer index or NumPy array of integer indices corresponding to the input value(s).
        """
        return fn((v - self.offset) / self.gain).astype(int)

    @classmethod
    def create_time_axis(cls, fs: float, offset: float = 0.0) -> "LinearAxis":
        """
        Convenience method to construct a `LinearAxis` representing time.

        Args:
            fs: The sampling frequency in Hertz. The gain will be `1.0 / fs`.
            offset: The time offset of the first sample in seconds. Defaults to 0.0.

        Returns:
            A `LinearAxis` instance configured for time, with `unit="s"`.
        """
        return cls(unit="s", gain=1.0 / fs, offset=offset)


@dataclass
class ArrayWithNamedDims:
    """
    A simple dataclass that pairs a NumPy array with a list of dimension names.

    Attributes:
        data: The NumPy array.
        dims: A list of strings representing the names of the dimensions.
              The length of `dims` must match `data.ndim`.
              Dimension names must be unique.
    """
    data: npt.NDArray
    dims: typing.List[str]

    def __post_init__(self):
        if len(self.dims) != self.data.ndim:
            raise ValueError("dims must be same length as data.shape")
        if len(self.dims) != len(set(self.dims)):
            raise ValueError("dims contains repeated dim names")

    def __eq__(self, other):
        """
        Checks for equality with another `ArrayWithNamedDims` object.

        Two objects are considered equal if they are of the same class,
        have the same dimension names in the same order, and their data arrays are equal.

        Returns:
            True if the objects are equal, False otherwise, or NotImplemented.
        """
        if self is other:
            return True
        if other.__class__ is self.__class__:
            if self.dims == other.dims and np.array_equal(self.data, other.data):
                return True
        return NotImplemented


@dataclass(eq=False)
class CoordinateAxis(AxisBase, ArrayWithNamedDims):
    """
    An axis where the coordinate values are explicitly provided by another array.

    This is useful for non-linear or irregularly spaced coordinates.
    It inherits from `ArrayWithNamedDims` to store its own coordinate data and dimension names.

    Attributes:
        data: A NumPy array holding the coordinate values.
        dims: A list of dimension names for the `data` array.
        unit: The physical unit of the coordinate values.
    """
    @typing.overload
    def value(self, x: int) -> typing.Any: ...
    @typing.overload
    def value(self, x: npt.NDArray[np.int_]) -> npt.NDArray: ...
    def value(self, x):
        """
        Retrieves the coordinate value(s) at the given index/indices from its internal `data` array.

        Args:
            x: An integer index or a NumPy array of integer indices.

        Returns:
            The coordinate value(s) from the `data` array at the specified index/indices.
        """
        return self.data[x]


@dataclass(eq=False)
class AxisArray(ArrayWithNamedDims):
    """
    A multi-dimensional array with named dimensions, typed axes, and attributes.

    `AxisArray` is designed to be a lightweight, self-describing data structure,
    similar in concept to `xarray.DataArray` but with a simpler API and fewer
    dependencies, suitable for messaging systems like `ezmsg`.

    Attributes:
        data: The underlying NumPy array holding the data.
        dims: A list of strings naming each dimension of the `data` array.
        axes: A dictionary mapping dimension names to `AxisBase` objects that
              describe the coordinates or scale of that dimension.
        attrs: A dictionary for storing arbitrary metadata as key-value pairs.
        key: A string identifier for the `AxisArray`, useful for filtering or routing.
    """

    axes: typing.Dict[str, AxisBase] = field(default_factory=dict)
    attrs: typing.Dict[str, typing.Any] = field(default_factory=dict)
    key: str = ""

    def __eq__(self, other):
        """
        Checks for equality with another `AxisArray` object.

        Extends `ArrayWithNamedDims.__eq__` by also comparing `key`, `attrs`, and `axes`.
        Two `AxisArray` objects are considered equal if all these fields,
        in addition to `dims` and `data`, are equal.

        Returns:
            True if the objects are equal, False otherwise, or NotImplemented.
        """
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
        """
        Deprecated alias for `LinearAxis`.

        This class is provided for backward compatibility and will be removed in a future version.
        Use `LinearAxis` instead.
        """
        # deprecated backward compatibility
        def __post_init__(self) -> None:
            warnings.warn(
                "AxisArray.Axis is a deprecated alias for LinearAxis",
                DeprecationWarning,
                stacklevel=2,
            )

        @classmethod
        def TimeAxis(cls, fs: float, offset: float = 0.0) -> "AxisArray.Axis":
            """
            Deprecated. Use `LinearAxis.create_time_axis(fs, offset)` or `AxisArray.TimeAxis(fs, offset)` instead.
            Creates a time axis with specified sampling frequency and offset.
            """
            return cls(unit="s", gain=1.0 / fs, offset=offset)

        def units(self, idx):
            """
            Deprecated. Use `value(idx)` instead.
            Calculates the physical value for a given index.
            """
            # NOTE: This is poorly named anyway, it should have been `value`
            return (idx * self.gain) + self.offset

        def index(self, val):
            """
            Deprecated. Use `index(val)` on a `LinearAxis` instance instead.
            Calculates the array index for a given physical value.
            """
            return np.round((val - self.offset) / self.gain)

    TimeAxis = LinearAxis.create_time_axis
    """
    Alias for `LinearAxis.create_time_axis`.

    Convenience method to construct a `LinearAxis` representing time.
    Example: `AxisArray.TimeAxis(fs=100.0, offset=0.1)`
    """
    LinearAxis = LinearAxis
    CoordinateAxis = CoordinateAxis

    @dataclass(frozen=True)
    class AxisInfo:
        axis: AxisBase
        """
        Provides information about a specific axis of an `AxisArray`.

        This is typically obtained via the `AxisArray.ax()` method.

        Attributes:
            axis: The `AxisBase` object describing the axis.
            idx: The integer index of this axis in the `AxisArray.dims` list.
            size: The size of this axis (length of the dimension).
        """
        idx: int
        size: typing.Optional[int] = None

        def __post_init__(self) -> None:
            if not isinstance(self.axis, CoordinateAxis) and self.size is None:
                raise ValueError("must define size if not using CoordinateAxis")
            elif isinstance(self.axis, CoordinateAxis) and self.size is not None:
                raise ValueError("must not define size if using CoordinateAxis")

        def __len__(self) -> int:
            """
            Returns the size of the axis.
            """
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
            Returns a NumPy array of integer indices for this axis, from `0` to `len(self) - 1`.
            """
            return np.arange(len(self))

        @property
        def values(self) -> npt.NDArray:
            """
            Returns a NumPy array of the physical coordinate values for this axis.
            """
            return self.axis.value(self.indices)

    def isel(
        self: _AxisArrayType,
        indexers: typing.Optional[typing.Any] = None,
        drop: bool = False,
        **indexers_kwargs: typing.Any,
    ) -> _AxisArrayType:
        """
        Selects data by integer indices along specified dimensions.

        Args:
            indexers: A dictionary where keys are dimension names and values are
                      integer indices, slices, or NumPy arrays of integer indices.
            drop: If `True`, dimensions indexed by a single integer will be
                  dropped. If `False` (default), they will be preserved as
                  singleton dimensions (size 1).
            **indexers_kwargs: Alternative way to specify indexers as keyword arguments.

        Returns:
            A new `AxisArray` containing the selected data. Axes are updated accordingly.

        Raises:
            ValueError: If an indexer type is invalid.
        """
        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "isel")

        # Prepare for constructing the new state
        resulting_axes = self.axes.copy()
        
        # np_idx_constructor will hold the actual objects used for NumPy indexing
        np_idx_constructor: typing.List[typing.Any] = [slice(None)] * self.data.ndim
        
        # resulting_dims_list will be the list of dimension names for the new AxisArray
        resulting_dims_list: typing.List[str] = [] 

        for original_ax_idx, original_dim_name in enumerate(self.dims):
            # Determine if and how this dimension is being indexed by the user
            user_ix_spec = indexers.get(original_dim_name, indexers.get(original_ax_idx))

            if user_ix_spec is None:
                # This dimension is not indexed by the user; it's kept as is.
                resulting_dims_list.append(original_dim_name)
                # np_idx_constructor[original_ax_idx] is already slice(None)
                continue

            # This dimension IS being indexed. Validate the indexer type.
            if not isinstance(user_ix_spec, (np.ndarray, int, slice)):
                raise ValueError(
                    f"Indexer for dimension '{original_dim_name}' must be an int, slice, or ndarray, "
                    f"got {type(user_ix_spec)}."
                )

            ax_info = self.ax(original_dim_name) # Needed for axis updates

            if isinstance(user_ix_spec, int):
                if drop:
                    # Dimension is dropped: use the integer directly for NumPy indexing.
                    np_idx_constructor[original_ax_idx] = user_ix_spec
                    # Remove its axis object if it exists.
                    if original_dim_name in resulting_axes:
                        del resulting_axes[original_dim_name]
                    # The dimension name is NOT added to resulting_dims_list.
                else:
                    # Convert negative index to positive for consistent slice creation
                    dim_size = ax_info.size
                    normalized_idx = user_ix_spec if user_ix_spec >= 0 else user_ix_spec + dim_size
                    if not (0 <= normalized_idx < dim_size):
                        raise IndexError(f"Index {user_ix_spec} is out of bounds for dimension '{original_dim_name}' with size {dim_size}")

                    # Dimension is kept as singleton: use a slice for NumPy indexing.
                    np_idx_constructor[original_ax_idx] = slice(normalized_idx, normalized_idx + 1)
                    resulting_dims_list.append(original_dim_name) # Add to kept dims.
                    # Update axis offset if it's a LinearAxis.
                    if original_dim_name in resulting_axes and isinstance(resulting_axes[original_dim_name], LinearAxis):
                        new_offset = resulting_axes[original_dim_name].value(normalized_idx)
                        resulting_axes[original_dim_name] = replace(
                            resulting_axes[original_dim_name], offset=new_offset
                        )
            
            elif isinstance(user_ix_spec, (slice, np.ndarray)):
                # Dimension is sliced or indexed by an array; it's kept.
                np_idx_constructor[original_ax_idx] = user_ix_spec
                resulting_dims_list.append(original_dim_name) # Add to kept dims.
                
                # Update axis offset if LinearAxis.
                if original_dim_name in resulting_axes and isinstance(resulting_axes[original_dim_name], LinearAxis):
                    current_dim_indices_for_axis_obj = np.arange(ax_info.size)
                    if isinstance(user_ix_spec, slice):
                        selected_indices_for_axis_update = current_dim_indices_for_axis_obj[user_ix_spec]
                    else: # np.ndarray (assumed to be integer indices)
                        selected_indices_for_axis_update = np.take(
                            current_dim_indices_for_axis_obj, user_ix_spec, axis=0
                        )
                    
                    if len(selected_indices_for_axis_update) > 0:
                        new_offset = resulting_axes[original_dim_name].value(selected_indices_for_axis_update[0])
                        resulting_axes[original_dim_name] = replace(
                            resulting_axes[original_dim_name], offset=new_offset
                        )
                    elif original_dim_name in resulting_axes: # Dimension becomes size 0.
                        del resulting_axes[original_dim_name]
                            
        resulting_data = self.data[tuple(np_idx_constructor)]

        # Ensure axes in resulting_axes correspond to dimensions in resulting_dims_list.
        final_pruned_axes = {
            k: v for k, v in resulting_axes.items() if k in resulting_dims_list
        }

        return replace(self, data=resulting_data, dims=resulting_dims_list, axes=final_pruned_axes)

    def sel(
        self: _AxisArrayType,
        indexers: typing.Optional[typing.Any] = None,
        drop: bool = False,
        **indexers_kwargs: typing.Any,
    ) -> _AxisArrayType:
        """
        Selects data by coordinate values along specified dimensions.

        This method currently only supports slices for `LinearAxis` types.
        If `drop` is True and a slice selects a single element, the dimension
        will be dropped.

        Args:
            indexers: A dictionary where keys are dimension names and values are slices
                      of coordinate values.
            **indexers_kwargs: Alternative way to specify indexers as keyword arguments.

        Returns:
            A new `AxisArray` containing the selected data. Axes are updated accordingly.

        Raises:
            ValueError: If an indexer is not a slice or if the axis is not a `LinearAxis`.
        """
        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "sel")

        out_indexers_for_isel = dict()
        for axis_name, ix_coord_slice in indexers.items():
            axis = self.get_axis(axis_name)

            if not isinstance(ix_coord_slice, slice):
                raise ValueError("sel only supports slices for now")
            if not isinstance(axis, LinearAxis):
                raise ValueError("sel only supports LinearAxis for now")
            
            # Convert coordinate slice to integer-based slice components
            start_idx = axis.index(ix_coord_slice.start) if ix_coord_slice.start is not None else None
            stop_idx = axis.index(ix_coord_slice.stop) if ix_coord_slice.stop is not None else None
            
            step_idx: typing.Optional[int] = None
            if ix_coord_slice.step is not None:
                if axis.gain == 0:
                    raise ValueError(f"LinearAxis '{axis_name}' has gain=0, cannot calculate step for sel.")
                
                index_step_float = ix_coord_slice.step / axis.gain
                step_idx = int(np.rint(index_step_float))
                
                if step_idx == 0:
                    if ix_coord_slice.step == 0: # User explicitly asked for step 0
                        raise ValueError(f"Coordinate step for axis '{axis_name}' is 0, which is invalid for a slice.")
                    else: # Calculated index step rounded to 0, but original coord step was non-zero
                        raise ValueError(
                            f"Coordinate step {ix_coord_slice.step} for axis '{axis_name}' "
                            f"with gain {axis.gain} results in an index step of {index_step_float}, "
                            "which rounds to 0. Slice steps must be non-zero integers."
                        )

            int_indexer_for_dim: typing.Union[slice, int] = slice(start_idx, stop_idx, step_idx)

            out_indexers_for_isel[axis_name] = int_indexer_for_dim
            # The `drop` argument is passed to isel, which will only act on it
            # if the int_indexer_for_dim for a particular axis is an integer.
        
        return self.isel(indexers=out_indexers_for_isel, drop=drop)

    def modify_dims(self: _AxisArrayType, name_map: typing.Dict[str, typing.Optional[str]]) -> _AxisArrayType:
        """
        Renames and/or drops dimensions.

        Args:
            name_map: A dictionary mapping current dimension names to new names (str)
                      or to None (to drop the dimension).

        Returns:
            A new AxisArray with modified dimensions and corresponding axes.

        Raises:
            ValueError: If attempting to drop a non-singleton dimension or
                        if renaming results in duplicate dimension names.
        """
        if not name_map:
            return self

        renames = {old: new for old, new in name_map.items() if isinstance(new, str)}
        dims_to_drop = {old for old, new in name_map.items() if new is None}

        final_dims_list: typing.List[str] = []
        original_kept_dims_for_axes: typing.List[str] = []

        for dim_name in self.dims:
            if dim_name in dims_to_drop:
                continue
            new_name = renames.get(dim_name, dim_name)
            final_dims_list.append(new_name)
            original_kept_dims_for_axes.append(dim_name)

        new_axes = {}
        for original_dim, final_dim_name in zip(original_kept_dims_for_axes, final_dims_list):
            if original_dim in self.axes:
                new_axes[final_dim_name] = self.axes[original_dim]

        drop_indices = []
        for i, dim_name in enumerate(self.dims):
            if dim_name in dims_to_drop:
                if self.data.shape[i] != 1:
                    raise ValueError(
                        f"Cannot drop dimension '{dim_name}' with size {self.data.shape[i]} != 1. "
                        "Only singleton dimensions can be dropped automatically."
                    )
                drop_indices.append(i)

        new_data = self.data
        if drop_indices:
            new_data = np.squeeze(self.data, axis=tuple(drop_indices))

        return replace(self, data=new_data, dims=final_dims_list, axes=new_axes)

    def update_axes(self: _AxisArrayType, **axes_to_update: AxisBase) -> _AxisArrayType:
        """
        Updates or assigns AxisBase objects for specified dimensions.

        Args:
            **axes_to_update: Keyword arguments where keys are dimension names
                              and values are the new AxisBase objects.

        Returns:
            A new AxisArray with updated axes.

        Raises:
            ValueError: If a specified dimension name does not exist.
            TypeError: If a provided axis object is not an instance of AxisBase.
        """
        if not axes_to_update:
            return self

        new_axes = self.axes.copy()
        for dim_name, axis_obj in axes_to_update.items():
            if dim_name not in self.dims:
                raise ValueError(f"Dimension '{dim_name}' not found in AxisArray dims: {self.dims}")
            if not isinstance(axis_obj, AxisBase):
                raise TypeError(f"Axis for '{dim_name}' must be an instance of AxisBase, got {type(axis_obj)}")
            new_axes[dim_name] = axis_obj

        return replace(self, axes=new_axes)

    def update_attrs(self: _AxisArrayType, **attrs_to_update: typing.Any) -> _AxisArrayType:
        """
        Updates or adds attributes to the `attrs` dictionary.

        Args:
            **attrs_to_update: Keyword arguments where keys are attribute names
                               and values are the new attribute values.

        Returns:
            A new `AxisArray` with the updated attributes.
        """
        new_attrs = self.attrs.copy()
        new_attrs.update(attrs_to_update)
        return replace(self, attrs=new_attrs)

    @property
    def shape(self) -> typing.Tuple[int, ...]:
        """Returns the shape of the underlying `data` array as a tuple."""
        return self.data.shape

    def to_xr_dataarray(self) -> "DataArray":
        from xarray import DataArray

        coords = {}
        for name, axis in self.axes.items():
            if name in self.dims:
                coords[name] = (name, self.ax(name).values)
            elif isinstance(axis, CoordinateAxis):
                coords[name] = (axis.dims, axis.data)

        return DataArray(self.data, coords=coords, dims=self.dims, attrs=self.attrs)

    def ax(self, dim: typing.Union[str, int]) -> AxisInfo:
        """
        Retrieves information about a specified axis/dimension.

        Args:
            dim: The name (str) or integer index of the dimension.

        Returns:
            An `AxisInfo` object containing the `AxisBase` object,
            integer index, and size of the specified dimension.
        """
        axis_idx = dim if isinstance(dim, int) else self.get_axis_idx(dim)
        axis_name = dim if isinstance(dim, str) else self.get_axis_name(dim)
        return AxisArray.AxisInfo(
            axis=self.get_axis(axis_name), idx=axis_idx, size=self.shape[axis_idx]
        )

    def get_axis(self, dim: typing.Union[str, int]) -> AxisBase:
        """
        Gets the `AxisBase` object associated with a dimension.

        Args:
            dim: The name (str) or integer index of the dimension.

        Returns:
            The `AxisBase` object for the specified dimension. Returns a default
            `LinearAxis()` if no specific axis is defined for that dimension
            (for backward compatibility).
        """
        if isinstance(dim, int):
            dim = self.get_axis_name(dim)
        if dim not in self.dims:
            raise ValueError(f"{dim=} not present in object")
        return self.axes.get(dim, LinearAxis())  # backward compat

    def get_axis_name(self, dim: int) -> str:
        """
        Gets the name of a dimension by its integer index.

        Args:
            dim: The integer index of the dimension.

        Returns:
            The name (str) of the dimension.
        """
        return self.dims[dim]

    def get_axis_idx(self, dim: str) -> int:
        """
        Gets the integer index of a dimension by its name.

        Args:
            dim: The name (str) of the dimension.

        Returns:
            The integer index of the dimension.

        Raises:
            ValueError: If the dimension name is not found.
        """
        try:
            return self.dims.index(dim)
        except ValueError:
            raise ValueError(f"{dim=} not present in object")

    def axis_idx(self, dim: typing.Union[str, int]) -> int:
        """
        Returns the integer index of a dimension.

        If `dim` is a string, it's converted to an index. If it's already an integer,
        it's returned directly.

        Args:
            dim: The name (str) or integer index of the dimension.

        Returns:
            The integer index of the dimension.
        """
        return self.get_axis_idx(dim) if isinstance(dim, str) else dim

    def _axis_idx(self, dim: typing.Union[str, int]) -> int:
        """Deprecated. Use `axis_idx` instead."""
        return self.axis_idx(dim)

    def as2d(self, dim: typing.Union[str, int]) -> npt.NDArray:
        """
        Reshapes the `data` array into a 2D array where the specified dimension `dim`
        becomes the first dimension, and all other dimensions are collapsed into the second.

        Args:
            dim: The dimension (name or index) to be preserved as the first dimension.

        Returns:
            A 2D NumPy array view or copy.
        """
        return as2d(self.data, self.axis_idx(dim))

    def iter_over_axis(
        self: _AxisArrayType, axis: typing.Union[str, int]
    ) -> typing.Generator[_AxisArrayType, None, None]:
        """
        Iterates over slices of the `AxisArray` along a specified axis.

        Each yielded item is a new `AxisArray` representing a slice, with the
        iterated dimension removed.

        Args:
            axis: The dimension (name or index) to iterate over.

        Yields:
            `AxisArray` objects, each representing a slice along the specified axis.
        """
        axis_idx = self.axis_idx(axis)
        dim_name = self.dims[axis_idx]
        new_dims = [d for i, d in enumerate(self.dims) if i != axis_idx]
        new_axes = {d: a for d, a in self.axes.items() if d != dim_name}

        for it_data in np.moveaxis(self.data, axis_idx, 0):
            it_aa = replace(
                self,
                data=it_data,
                dims=new_dims,
                axes=new_axes,
            )
            yield it_aa

    @contextmanager
    def view2d(
        self, dim: typing.Union[str, int]
    ) -> typing.Generator[npt.NDArray, None, None]:
        """
        Provides a 2D view of the `data` array as a context manager.

        The specified dimension `dim` becomes the first dimension, and all other
        dimensions are collapsed into the second. Changes to the yielded array
        may be reflected in the original `data` if a view is possible.

        Args:
            dim: The dimension (name or index) to be preserved as the first dimension.

        Yields:
            A 2D NumPy array view or copy.
        """
        with view2d(self.data, self.axis_idx(dim)) as arr:
            yield arr

    def shape2d(self, dim: typing.Union[str, int]) -> typing.Tuple[int, int]:
        """
        Calculates the shape the `data` array would have if reshaped by `as2d(dim)`.
        Returns a tuple (size_of_dim, size_of_all_other_dims_combined).
        """
        return shape2d(self.data, self.axis_idx(dim))

    # TODO: concatenate/transpose should be removed in a future version
    @staticmethod
    def concatenate(
        *aas: _AxisArrayType,
        dim: str,
        axis: typing.Optional[AxisBase] = None,
        filter_key: typing.Optional[str] = None,
    ) -> _AxisArrayType:
        """
        Deprecated. Use the module-level `concatenate` function instead.
        Concatenates multiple `AxisArray` objects along a specified dimension.
        """
        warnings.warn(
            "AxisArray.concatenate is deprecated. Use the module-level 'concatenate' function instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return concatenate(*aas, dim=dim, axis=axis, filter_key=filter_key) # type: ignore

    @staticmethod
    def transpose(aa: _AxisArrayType, dims: typing.Optional[typing.Union[typing.Iterable[str], typing.Iterable[int]]] = None) -> _AxisArrayType:
        warnings.warn(
            """
            Deprecated. Use the module-level `transpose` function instead.
            Transposes the `AxisArray` by permuting its dimensions.
            """
            "AxisArray.transpose is deprecated. Use the module-level 'transpose' function instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return transpose(aa, dims=dims) # type: ignore


def slice_along_axis(
    in_arr: npt.NDArray, sl: typing.Union[slice, int], axis: int
) -> npt.NDArray:
    """
    Slice the input array along a specified axis using the given slice object or integer index.
     Integer arguments to `sl` will cause the sliced dimension to be dropped.
     Use `slice(my_int, my_int+1, None)` to keep the sliced dimension.

    Parameters:
        in_arr: The input array to be sliced.
        sl: The slice object or integer index to use for slicing.
        axis: The axis along which to slice the array.

    Returns:
        The sliced array (view).

    Raises:
        ValueError: If the axis value is invalid for the input array.
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

    Args:
        in_arr: The input array.
        nwin: The size of the sliding window.
        axis: The axis along which the sliding window will be applied.
        step: The size of the step between windows. If > 1, the strided window will be sliced with `slice_along_axis`

    Returns:
        A view to the input array with the sliding window applied.

    Note: There is a known edge case when nwin == shape[axis] + 1. While this should raise
        an error because the window is larger than the input, the implementation ends up
        returning a 0-length window. We could check for this but this function is intended
        to have minimal latency so we have decided to skip the checks and deal with the
        support issues as they arise.
    """
    if -in_arr.ndim <= axis < 0:
        axis = in_arr.ndim + axis
    out_shape = (
        in_arr.shape[:axis]
        + (in_arr.shape[axis] - (nwin - 1),)
        + (nwin,)
        + in_arr.shape[axis + 1 :]
    )
    out_strides = (
        in_arr.strides[:axis] + (in_arr.strides[axis],) * 2 + in_arr.strides[axis + 1 :]
    )
    result = nps.as_strided(
        in_arr, strides=out_strides, shape=out_shape, writeable=False
    )
    if step > 1:
        result = slice_along_axis(result, slice(None, None, step), axis)
    return result


def _as2d(
    in_arr: npt.NDArray, axis: int = 0
) -> typing.Tuple[npt.NDArray, typing.Tuple[int, ...]]:
    """
    Helper function to reshape an array into 2D and return the original remaining shape.

    The specified `axis` becomes the first dimension of the 2D array,
    and all other dimensions are collapsed into the second dimension.

    Args:
        in_arr: The input NumPy array.
        axis: The axis to become the first dimension. Defaults to 0.

    Returns:
        A tuple containing the 2D reshaped array and the shape of the collapsed dimensions.
    """
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
    """
    Reshapes an array into a 2D array.

    The specified `axis` becomes the first dimension of the 2D array,
    and all other dimensions are collapsed into the second dimension.

    Args:
        in_arr: The input NumPy array.
        axis: The axis to become the first dimension. Defaults to 0.

    Returns:
        A 2D reshaped NumPy array (potentially a view or a copy).
    """
    arr, _ = _as2d(in_arr, axis=axis)
    return arr


@contextmanager
def view2d(
    in_arr: npt.NDArray, axis: int = 0
) -> typing.Generator[npt.NDArray, None, None]:
    """
    2D-View (el x ch) of the array, no matter what input dimensionality is.
    Yields a view of underlying data when possible, changes to data in yielded
    array will always be reflected in self.data, either via the view or copying
    NOTE: In practice, I'm not sure this is very useful because it requires modifying
    the numpy array data in-place, which limits its application to zero-copy messages
    """
    arr, remaining_shape = _as2d(in_arr, axis=axis)

    yield arr

    if not np.shares_memory(arr, in_arr):
        arr = arr.reshape(arr.shape[0], *remaining_shape)
        arr = np.moveaxis(arr, 0, axis)
        in_arr[:] = arr


def shape2d(arr: npt.NDArray, axis: int = 0) -> typing.Tuple[int, int]:
    """
    Calculates the shape an array would have if it were reshaped into 2D
    using `as2d` with the specified axis.

    Args:
        arr: The input NumPy array.
        axis: The axis that would become the first dimension. Defaults to 0.

    Returns:
        A tuple (shape_of_axis, product_of_other_shapes).
    """
    return (
        arr.shape[axis],
        math.prod(v for idx, v in enumerate(arr.shape) if idx != axis),
    )


def concatenate(
    *aas: _AxisArrayType,
    dim: str,
    axis: typing.Optional[AxisBase] = None,
    filter_key: typing.Optional[str] = None,
) -> _AxisArrayType:
    """
    Concatenates multiple `AxisArray` objects along a specified dimension.

    All input `AxisArray` objects must have compatible dimensions (excluding the
    concatenation dimension if it's new). If `dim` is an existing dimension,
    arrays are concatenated along this dimension. If `dim` is a new dimension name,
    the arrays are stacked along a new first dimension named `dim`.

    Args:
        *aas: A variable number of `AxisArray` objects to concatenate.
        dim: The name of the dimension along which to concatenate or stack.
        axis: An optional `AxisBase` object to assign to the `dim` if it's a new
              dimension or to update an existing one.
        filter_key: If provided, only `AxisArray` objects with a matching `key` attribute
                    will be included in the concatenation.

    Returns:
        A new `AxisArray` object resulting from the concatenation.
    """
    if not aas:
        # Raise error if no arrays are provided initially, even before filtering
        # This matches behavior if aas[0] was accessed on an empty tuple.
        raise ValueError("No AxisArrays provided to concatenate.")

    if filter_key is not None:
        filtered_aas = tuple([aa for aa in aas if aa.key == filter_key])
        if not filtered_aas:
            # If filtering results in an empty list, raise an error.
            # Original code would have raised IndexError on aas[0] if filtering emptied aas.
            raise ValueError(f"No AxisArrays remaining after filtering for key='{filter_key}'.")
        aas = filtered_aas # type: ignore

    aa_0 = aas[0]
    for aa_other in aas[1:]:
        if aa_other.dims != aa_0.dims:
            raise ValueError("Objects have incompatible dims for concatenation.")

    new_dims = list(aa_0.dims)
    new_axes = dict(aa_0.axes)
    new_key = aa_0.key if all(aa.key == aa_0.key for aa in aas) else ""

    if axis is not None:
        new_axes[dim] = axis

    all_data = [aa.data for aa in aas]

    if dim in aa_0.dims:
        dim_idx = aa_0.axis_idx(dim=dim)
        new_data = np.concatenate(all_data, axis=dim_idx)
    else:
        # New dimension is being created
        new_data = np.stack(all_data, axis=0)  # Stack along a new first axis
        new_dims.insert(0, dim)  # New dimension becomes the first dimension

    return replace(aa_0, data=new_data, dims=new_dims, axes=new_axes, key=new_key)


def transpose(aa: _AxisArrayType, dims: typing.Optional[typing.Union[typing.Iterable[str], typing.Iterable[int]]] = None) -> _AxisArrayType:
    """
    Reorders the dimensions of an `AxisArray`.

    This is analogous to `numpy.transpose`.

    Args:
        aa: The input `AxisArray`.
        dims: An iterable of dimension names (str) or integer indices specifying
              the new order of dimensions. If None, the dimensions are reversed.
              Must be a permutation of the existing dimensions.

    Returns:
        A new `AxisArray` with transposed data and reordered `dims`. `axes` are preserved.
    """
    if dims is None:
        # Default: reverse the order of dimensions
        perm = list(reversed(range(aa.data.ndim)))
    else:
        perm = [aa.axis_idx(d) for d in dims]

    if len(perm) != aa.data.ndim or len(set(perm)) != aa.data.ndim:
        raise ValueError("dims argument must be a permutation of existing dimension indices/names")

    new_dims = [aa.dims[idx] for idx in perm]
    new_data = np.transpose(aa.data, perm)
    # Axes dictionary (aa.axes) remains valid as it's keyed by dim names,
    # and dim names are preserved (just reordered in new_dims).
    return replace(aa, data=new_data, dims=new_dims, axes=aa.axes)
