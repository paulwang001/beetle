use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::io::Read;
use bytes::Bytes;
use image::{EncodableLayout, ImageBuffer, ImageFormat, Rgb, RgbImage};
use rand::Rng;
use crate::avatar_nickname::hash::my_hash;
use crate::avatar_nickname::nibbler::Nibbler;

// fn random_str() -> String{
//     let random_string: String = rand::thread_rng().sample_iter(&rand::distributions::Alphanumeric)
//         .take(10)
//         .map(char::from)
//         .collect();
//     random_string
// }
struct IdenticonDEV<'a>{
    source: &'a [u8],
    size: u32,
}

impl<'a> IdenticonDEV<'a> {
    pub fn new(source: &[u8]) -> IdenticonDEV {
        IdenticonDEV {
            source: source,
            size: 420,
        }
    }

    // https://processing.org/reference/map_.html
    fn map(value: u32, vmin: u32, vmax: u32, dmin: u32, dmax: u32) -> f32 {
        ((value - vmin) * (dmax - dmin)) as f32 / ((vmax - vmin) + dmin) as f32
    }

    fn foreground(&self) -> Rgb<u8> {
        // Use last 28 bits to determine HSL values.
        let h1 = (self.source[12] as u16 & 0x0f) << 8;
        let h2 = self.source[13] as u16;

        let h = (h1 | h2) as u32;
        let s = self.source[14] as u32;
        let l = self.source[15] as u32;

        let hue = IdenticonDEV::map(h, 0, 4095, 0, 360);
        let sat = IdenticonDEV::map(s, 0, 255, 0, 20);
        let lum = IdenticonDEV::map(l, 0, 255, 0, 20);

        HSL::new(hue, 65.0 - sat, 75.0 - lum).rgb()
    }

    fn rect(image: &mut RgbImage, x0: u32, y0: u32, x1: u32, y1: u32, color: Rgb<u8>) {
        for x in x0..x1 {
            for y in y0..y1 {
                image.put_pixel(x, y, color);
            }
        }
    }

    fn pixels(&self) -> [bool; 25] {
        let mut nibbles = Nibbler::new(self.source).map(|x| x % 2 == 0);
        let mut pixels = [false; 25];
        for col in (0..3).rev() {
            for row in 0..5 {
                let ix = col + (row * 5);
                let mirror_col = 4 - col;
                let mirror_ix = mirror_col + (row * 5);
                let paint = nibbles.next().unwrap_or(false);
                pixels[ix] = paint;
                pixels[mirror_ix] = paint;
            }
        }
        pixels
    }

    pub fn image(&self,background_rgb: Rgb<u8>) -> RgbImage {
        let pixel_size = 70;
        let sprite_size = 5;
        let margin = pixel_size / 2;

        let background = background_rgb.clone();
        let foreground = self.foreground();

        let mut image: RgbImage = ImageBuffer::from_pixel(self.size, self.size, background);

        for (row, pix) in self.pixels().chunks(sprite_size).enumerate() {
            for (col, painted) in pix.iter().enumerate() {
                if *painted {
                    let x = col * pixel_size;
                    let y = row * pixel_size;
                    IdenticonDEV::rect(
                        &mut image,
                        (x + margin) as u32,
                        (y + margin) as u32,
                        (x + pixel_size + margin) as u32,
                        (y + pixel_size + margin) as u32,
                        foreground,
                    );
                }
            }
        }

        image
    }
}

pub struct HSL {
    hue: f32,
    sat: f32,
    lum: f32,
}

impl HSL {
    pub fn new(hue: f32, sat: f32, lum: f32) -> HSL {
        HSL {
            hue: hue,
            sat: sat,
            lum: lum,
        }
    }

    // http://www.w3.org/TR/css3-color/#hsl-color
    pub fn rgb(&self) -> Rgb<u8> {
        let hue = self.hue / 360.0;
        let sat = self.sat / 100.0;
        let lum = self.lum / 100.0;

        let b = if lum <= 0.5 {
            lum * (sat + 1.0)
        } else {
            lum + sat - lum * sat
        };
        let a = lum * 2.0 - b;

        let r = HSL::hue_to_rgb(a, b, hue + 1.0 / 3.0);
        let g = HSL::hue_to_rgb(a, b, hue);
        let b = HSL::hue_to_rgb(a, b, hue - 1.0 / 3.0);

        Rgb([
            (r * 255.0).round() as u8,
            (g * 255.0).round() as u8,
            (b * 255.0).round() as u8,
        ])
    }

    fn hue_to_rgb(a: f32, b: f32, hue: f32) -> f32 {
        let h = if hue < 0.0 {
            hue + 1.0
        } else if hue > 1.0 {
            hue - 1.0
        } else {
            hue
        };

        if h < 1.0 / 6.0 {
            return a + (b - a) * 6.0 * h;
        }

        if h < 1.0 / 2.0 {
            return b;
        }

        if h < 2.0 / 3.0 {
            return a + (b - a) * (2.0 / 3.0 - h) * 6.0;
        }

        a
    }
}

pub fn generate_avatar(peer_id: & str) -> String {
    let background_rgb :[u8;3]= [240, 240, 240];
    let background : Rgb<u8> = Rgb(background_rgb);
    let hash = my_hash(peer_id.as_bytes());
    let a1 = hash.to_le_bytes();
    let a2 =a1.clone();
    let mut two_bytes : [u8;16]=[0; 16];
    let merged = two_bytes
        .iter()
        .take(a1.len() + a2.len())
        .enumerate()
        .map(|(i, _)| {
            if i < a1.len() {
                a1[i]
            } else {
                a2[i - a1.len()]
            }
        })
        .collect::<Vec<_>>();

    // println!("hash {}",&bytes);
    let mut image = IdenticonDEV::new(&merged).image(background);

    let mut buffer = std::io::Cursor::new(vec![]);
    image.write_to(&mut buffer, ImageFormat::Png).unwrap();
    format!("data:image/Png;base64,{}", base64::encode(buffer.get_ref()))
}