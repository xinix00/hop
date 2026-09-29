//! Een tabel met uitgelijnde kolommen, zoals Go's `tabwriter` met twee spaties opvulling.
//!
//! Bezit de rijen tot het schrijven; de breedte van een kolom is de langste
//! cel (in tekens, niet bytes: een graad-teken telt als één).

/// Een tabel.
#[derive(Debug, Default)]
pub(crate) struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    /// Een tabel met deze kop.
    pub(crate) fn new(head: &[&str]) -> Self {
        Self {
            rows: vec![head.iter().map(|s| String::from(*s)).collect()],
        }
    }

    /// Voegt een rij toe.
    pub(crate) fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    /// De tabel als tekst; de laatste kolom krijgt geen opvulling.
    pub(crate) fn render(&self) -> String {
        let cols = self.rows.iter().map(Vec::len).max().unwrap_or(0);
        let mut width = vec![0usize; cols];
        for r in &self.rows {
            for (w, c) in width.iter_mut().zip(r) {
                *w = (*w).max(c.chars().count());
            }
        }
        let mut out = String::new();
        for r in &self.rows {
            let last = r.len().saturating_sub(1);
            for (i, (c, w)) in r.iter().zip(&width).enumerate() {
                out.push_str(c);
                if i < last {
                    let pad = w - c.chars().count() + 2;
                    out.extend(std::iter::repeat_n(' ', pad));
                }
            }
            out.push('\n');
        }
        out
    }
}
