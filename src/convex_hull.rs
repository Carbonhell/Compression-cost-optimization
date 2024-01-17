/// Implementation from https://github.com/TheAlgorithms/Rust/blob/master/src/general/convex_hull.rs, slightly modified to be more generic
use std::cmp::Ordering::Equal;

pub trait Point {
    fn x(&self) -> f64;
    fn y(&self) -> f64;
}

fn sort_by_min_angle<'a, T: Point + PartialOrd>(pts: &[&'a T], min: &T) -> Vec<&'a T> {
    let mut points: Vec<(f64, f64, &T)> = pts
        .into_iter()
        .map(|x| {
            (
                (x.y() - min.y()).atan2(x.x() - min.x()),
                // angle
                (x.y() - min.y()).hypot(x.x() - min.x()),
                // distance (we want the closest to be first)
                *x,
            )
        })
        .collect();
    points.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));
    points.into_iter().map(|x| x.2).collect()
}

// calculates the z coordinate of the vector product of vectors ab and ac
fn calc_z_coord_vector_product<T: Point + PartialOrd>(a: &T, b: &T, c: &T) -> f64 {
    (b.x() - a.x()) * (c.y() - a.y()) - (c.x() - a.x()) * (b.y() - a.y())
}

/*
    If three points are aligned and are part of the convex hull then the three are kept.
    If one doesn't want to keep those points, it is easy to iterate the answer and remove them.

    The first point is the one with the lowest y-coordinate and the lowest x-coordinate.
    Points are then given counter-clockwise, and the closest one is given first if needed.
*/
pub fn convex_hull_graham<'a, T: Point + PartialOrd>(pts: &[&'a T]) -> Vec<&'a T> {
    if pts.is_empty() {
        return vec![];
    }

    let mut stack: Vec<&T> = vec![];
    let min = pts
        .iter()
        .min_by(|a, b| {
            let ord = a.y().partial_cmp(&b.y()).unwrap_or(Equal);
            match ord {
                Equal => a.x().partial_cmp(&b.x()).unwrap_or(Equal),
                o => o,
            }
        })
        .unwrap();
    let points = sort_by_min_angle(pts, min);

    if points.len() <= 3 {
        return points;
    }

    for point in points {
        while stack.len() > 1
            && calc_z_coord_vector_product(stack[stack.len() - 2], stack[stack.len() - 1], point)
            < 0.
        {
            stack.pop();
        }
        stack.push(point);
    }

    stack
}
