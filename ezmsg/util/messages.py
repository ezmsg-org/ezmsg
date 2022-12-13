import math

from contextlib import contextmanager
from dataclasses import field, dataclass

import numpy as np
import numpy.typing as npt

from typing import Generator, Tuple, Optional, List, Dict, Any

@dataclass
class Axis:
    ...

@dataclass
class DimensionalAxis(Axis):
    unit: str = ''
    gain: float = 1.0
    offset: float = 0.0

class TimeAxis(DimensionalAxis):
    def __init__( self, fs: float ) -> None:
        super().__init__( unit = 's', gain = 1.0 / fs )

    @property
    def fs( self ) -> float:
        return 1.0 / self.gain

@dataclass
class CategoricalAxis(Axis):
    labels: Optional[List[Any]] = None

@dataclass
class AxisArray:
    data: npt.NDArray
    dims: List[str] = field(default=None) # type: ignore
    axes: Dict[str, Axis] = field(default_factory=dict)
    coords: Dict[str, "AxisArray"] = field(default_factory=dict)

    _axis_num: Dict[ str, int ] = field(init = False)

    def __post_init__(self):

        self.data = np.array(self.data, ndmin=1)

        if self.dims is None:
            self.dims = [f'dim_{i}' for i in range(self.data.ndim)]
        if len(self.dims) != self.data.ndim:
            raise ValueError("dims must be same length as data.shape")
        self._axis_num = {ax_name: idx for (idx, ax_name) in enumerate(self.dims)}
            
        for coord_name, coord in self.coords.items():
            coord_shape: List[int] = []
            for cax_idx, cax_name in enumerate( coord.dims ):
                if cax_name not in self.dims:
                    raise ValueError(f'coord "{coord_name}" has dim "{cax_name}" which is not in self.dims')
                cax_dlen = self.shape[self._axis_num[cax_name]]
                if len( coord.shape ) <= cax_idx or cax_dlen != coord.shape[cax_idx]:
                    raise ValueError(f'"{cax_name}" shape does not match data')
                coord_shape.append(cax_dlen)

    @property
    def shape(self) -> Tuple[int, ...]:
        """ Shape of data """
        return self.data.shape

    def get_axis_num( self, dim: str ) -> int:
        return self._axis_num[ dim ]

    @contextmanager
    def view2d( self, dim: str ) -> Generator[np.ndarray, None, None]:
        with view2d( self.data, self.get_axis_num( dim ) ) as arr:
            yield arr

    def shape2d( self, dim: str ) -> Tuple[int, int]:
        return shape2d( self.data, self.get_axis_num( dim ) )

def concat( *aas: AxisArray, axis: str ) -> AxisArray:
    _axis_nums = aas[0]._axis_num
    for aa in aas[1:]:
        if aa._axis_num != _axis_nums:
            raise ValueError( 'AxisArrays have incompatible dimensions' )

    arr = np.concatenate( [ arr.data for arr in aas ], axis = aas[0]._axis_num[ axis ] )

    coords: Dict[ str, AxisArray ] = {}
    coord_names = set( [ coord_name for aa in aas for coord_name in aa.coords ] )
    for coord_name in coord_names:
        all_coord_data = []
        out_coord = aas[0].coords[ coord_name ]
        for aa in aas:
            coord_data = out_coord.data
            if coord_name in aa.coords:
                coord = aa.coords[ coord_name ]
                if coord.dims != out_coord.dims:
                    raise ValueError(f'Cannot concatenate; inconsistent axes in "{coord_name}"')
            else:
                coord_data = coord_data * np.nan
            all_coord_data.append( coord_data )
        
        axis_idx = 0
        out_coord_dims = out_coord.dims
        if axis in out_coord.dims:
            axis_idx = out_coord.dims.index( axis )
        else:
            all_coord_data = [ data[ np.newaxis, ... ] for data in all_coord_data ]
            out_coord_dims = [ axis ] + out_coord_dims
        all_coord_data = np.concatenate( all_coord_data, axis = axis_idx )

        coords[ coord_name ] = AxisArray( all_coord_data, out_coord_dims )   

    return AxisArray(arr, dims = aas[0].dims, axes = aas[0].axes, coords = coords)


@contextmanager
def view2d(in_arr: npt.NDArray, axis: int = 0) -> Generator[np.ndarray, None, None]:
    """ 
    2D-View (el x ch) of the array, no matter what input dimensionality is.
    Yields a view of underlying data when possible, changes to data in yielded 
    array will always be reflected in self.data, either via the view or copying
    """
    arr = in_arr
    if arr.ndim == 0:
        arr = arr.reshape(1, 1)
    elif arr.ndim == 1:
        arr = arr[:, np.newaxis]
    
    arr = np.moveaxis(arr, axis, 0)
    remaining_shape = arr.shape[1:]
    arr = arr.reshape(arr.shape[0], np.prod(remaining_shape))

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

# msg = AxisArray( 
#     np.ones( ( 2, 5, 128, 128 ) ), 
#     dims = [ 'sensor', 'time', 'x', 'y' ],
#     axes = {
#         'sensor': CategoricalAxis( labels = [ 'a', 'b' ] ),
#         'time': TimeAxis( fs = 5.0 ),
#         'x': DimensionalAxis( unit = 'mm', gain = 0.2, offset = -13.0 ),
#         'y': DimensionalAxis( unit = 'mm', gain = 0.2, offset = -13.0 )
#     }
# )

# def msg_gen( fs: float ) -> Generator[ AxisArray, None, None ]:
#     i = 0.0
#     while True:
#         i += 1.0
#         yield AxisArray( 
#             np.ones( ( 1, 64, 64 ) ) * i, 
#             dims = [ 'time', 'x', 'y' ],
#             axes = dict( 
#                 time = TimeAxis( fs = fs ),
#                 x = DimensionalAxis( unit = 'mm', gain = 0.2, offset = -13.0 ),
#                 y = DimensionalAxis( unit = 'mm', gain = 0.2, offset = -13.0 )
#             ),
#             coords = dict(
#                 timestamp = AxisArray( np.array( [ time.time() ] ), dims = [ 'time' ] )
#             )
#         )

# fs = 10.0
# gen = msg_gen( fs )
# win: List[ AxisArray ] = list()
# for msg, _ in zip( gen, range( 10 ) ):
#     time.sleep( 1.0 / fs )
#     win.append( msg )

# print( concat( *win, axis = 'time' ) )

