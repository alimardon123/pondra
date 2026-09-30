//! Rows as an Excel workbook (`/sql?format=xlsx`, the console's Download): one sheet, the columns'
//! names in bold in the first row, numbers and true/false as such and everything else as text.
//! The file is a zip of five small XML parts (Office Open XML), written here: no library for it.
use anyhow::{ensure, Result};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use std::fmt::Write as _;
use std::io::Write as _;

/// Excel's rows at most, the header's among them.
const MOST: usize = 1 << 20;

pub fn workbook(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    ensure!(n < MOST, "{n} rows: an Excel sheet holds at most {} (download CSV or Parquet instead)", MOST - 1);
    let mut sheet = String::from(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1">"#);
    if let Some(b) = batches.first() {
        for f in b.schema().fields() {
            write!(sheet, r#"<c t="inlineStr" s="1"><is><t>{}</t></is></c>"#, esc(f.name()))?;
        }
    }
    sheet.push_str("</row>");
    let mut r = 1;
    for b in batches {
        let opts = FormatOptions::default().with_null("");
        let shown: Vec<ArrayFormatter> = b.columns().iter().map(|c| ArrayFormatter::try_new(c.as_ref(), &opts)).collect::<Result<_, _>>()?;
        for i in 0..b.num_rows() {
            r += 1;
            write!(sheet, r#"<row r="{r}">"#)?;
            for (c, f) in b.columns().iter().zip(&shown) {
                if c.is_null(i) {
                    sheet.push_str("<c/>");
                } else if c.data_type() == &DataType::Boolean {
                    write!(sheet, r#"<c t="b"><v>{}</v></c>"#, c.as_boolean().value(i) as u8)?;
                } else if c.data_type().is_numeric() {
                    write!(sheet, "<c><v>{}</v></c>", f.value(i))?;
                } else {
                    write!(sheet, r#"<c t="inlineStr"><is><t xml:space="preserve">{}</t></is></c>"#, esc(&f.value(i).to_string()))?;
                }
            }
            sheet.push_str("</row>");
        }
    }
    sheet.push_str("</sheetData></worksheet>");
    const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    const REL: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    zip(&[
        ("[Content_Types].xml", format!(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>"#)),
        ("_rels/.rels", format!(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{REL}/officeDocument" Target="xl/workbook.xml"/></Relationships>"#)),
        ("xl/workbook.xml", format!(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="{MAIN}" xmlns:r="{REL}"><sheets><sheet name="Rows" sheetId="1" r:id="rId1"/></sheets></workbook>"#)),
        ("xl/_rels/workbook.xml.rels", format!(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{REL}/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="{REL}/styles" Target="styles.xml"/></Relationships>"#)),
        ("xl/styles.xml", format!(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="{MAIN}"><fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>"#)),
        ("xl/worksheets/sheet1.xml", sheet),
    ])
}

/// Text as XML holds it (and without the characters XML can't hold at all).
fn esc(s: &str) -> String {
    s.chars().filter(|c| !matches!(c, '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}')).fold(String::with_capacity(s.len()), |mut o, c| {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            c => o.push(c),
        }
        o
    })
}

/// A zip of these files, each deflated.
fn zip(files: &[(&str, String)]) -> Result<Vec<u8>> {
    let (mut out, mut dir) = (vec![], vec![]);
    for (name, text) in files {
        let mut z = flate2::write::DeflateEncoder::new(vec![], flate2::Compression::default());
        z.write_all(text.as_bytes())?;
        let packed = z.finish()?;
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, text.as_bytes()) as u32;
        ensure!(out.len() < u32::MAX as usize - packed.len() - 64, "too big for a workbook: download CSV or Parquet instead");
        let at = out.len() as u32;
        let head = |sig: u32, central: bool| {
            let mut h = sig.to_le_bytes().to_vec();
            if central {
                h.extend(20u16.to_le_bytes()); // (made by)
            }
            for v in [20u16, 0x0800, 8, 0, 0x21] { // (version, UTF-8 names, deflated, time, date: 1980-01-01)
                h.extend(v.to_le_bytes());
            }
            for v in [crc, packed.len() as u32, text.len() as u32] {
                h.extend(v.to_le_bytes());
            }
            h.extend((name.len() as u16).to_le_bytes());
            h.extend(0u16.to_le_bytes()); // (no extra field)
            if central {
                h.extend([0u8; 6]); // (no comment, disk 0, internal attributes)
                h.extend(0u32.to_le_bytes()); // (external attributes)
                h.extend(at.to_le_bytes());
            }
            h.extend(name.as_bytes());
            h
        };
        out.extend(head(0x04034b50, false));
        out.extend(&packed);
        dir.extend(head(0x02014b50, true));
    }
    let (start, size, n) = (out.len() as u32, dir.len() as u32, files.len() as u16);
    out.extend(dir);
    out.extend(0x06054b50u32.to_le_bytes());
    out.extend([0u8; 4]); // (this disk, the directory's disk)
    for v in [n, n] {
        out.extend(v.to_le_bytes());
    }
    for v in [size, start] {
        out.extend(v.to_le_bytes());
    }
    out.extend(0u16.to_le_bytes()); // (no comment)
    Ok(out)
}
