use arrow_array::{RecordBatch, StructArray};
use mosaico_core::{Ontology, Result};

use crate::columns::{OptionalStruct, RequiredStruct};
use crate::geometry::{
    Quaternion, QuaternionColumns, QuaternionView, Vector3d, Vector3dColumns, Vector3dView,
};
use crate::header::{Header, HeaderColumns, HeaderView};

#[derive(Debug, Clone, PartialEq)]
pub struct Imu {
    pub header: Option<Header>,
    pub acceleration: Vector3d,
    pub angular_velocity: Vector3d,
    pub orientation: Option<Quaternion>,
}

pub struct ImuColumns {
    header: OptionalStruct<HeaderColumns>,
    acceleration: RequiredStruct<Vector3dColumns>,
    angular_velocity: RequiredStruct<Vector3dColumns>,
    orientation: OptionalStruct<QuaternionColumns>,
}

#[derive(Clone, Copy)]
pub struct ImuView<'a> {
    columns: &'a ImuColumns,
    row: usize,
}

impl<'a> ImuView<'a> {
    pub fn header(&self) -> Option<HeaderView<'a>> {
        let columns = self.columns.header.get(self.row)?;
        Some(HeaderView::new(columns, self.row))
    }

    pub fn acceleration(&self) -> Result<Vector3dView<'a>> {
        let columns = self.columns.acceleration.get(self.row)?;
        Ok(Vector3dView::new(columns, self.row))
    }

    pub fn angular_velocity(&self) -> Result<Vector3dView<'a>> {
        let columns = self.columns.angular_velocity.get(self.row)?;
        Ok(Vector3dView::new(columns, self.row))
    }

    pub fn orientation(&self) -> Option<QuaternionView<'a>> {
        let columns = self.columns.orientation.get(self.row)?;
        Some(QuaternionView::new(columns, self.row))
    }

    pub fn to_model(&self) -> Result<Imu> {
        Ok(Imu {
            header: self.header().map(|h| h.to_model()).transpose()?,
            acceleration: self.acceleration()?.to_model()?,
            angular_velocity: self.angular_velocity()?.to_model()?,
            orientation: self.orientation().map(|o| o.to_model()).transpose()?,
        })
    }
}

impl Ontology for Imu {
    const TAG: &'static str = "IMU";
    type Columns = ImuColumns;
    type View<'a> = ImuView<'a>;

    fn columns(batch: &RecordBatch) -> Result<ImuColumns> {
        let array = StructArray::from(batch.clone());
        Ok(ImuColumns {
            header: OptionalStruct::new(&array, "", "header", HeaderColumns::new)?,
            acceleration: RequiredStruct::new(&array, "", "acceleration", Vector3dColumns::new)?,
            angular_velocity: RequiredStruct::new(
                &array,
                "",
                "angular_velocity",
                Vector3dColumns::new,
            )?,
            orientation: OptionalStruct::new(&array, "", "orientation", QuaternionColumns::new)?,
        })
    }

    fn view(columns: &ImuColumns, row: usize) -> ImuView<'_> {
        ImuView { columns, row }
    }

    fn from_view(view: &ImuView<'_>) -> Result<Imu> {
        view.to_model()
    }
}
