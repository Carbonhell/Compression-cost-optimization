use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};
use flate2::Compression;
use flate2::write::GzEncoder;
use rand::Rng;
use tempfile::tempfile;
use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::Workload;

#[derive(Debug)]
pub struct GzipCompressionLevel(pub u32);
#[derive(Debug)]
pub struct Gzip {
    compression_level: GzipCompressionLevel,
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>
}

impl Gzip {
    pub fn new(workload: &mut Workload, compression_level: GzipCompressionLevel, estimate_metadata: Option<EstimateMetadata>) -> Gzip {

        let mut gzip = Gzip {
            compression_level,
            compressed_size: None,
            time_required: None
        };
        gzip.calculate_metrics(workload, estimate_metadata);
        gzip
    }

    fn calculate_metrics(&mut self, workload: &mut Workload, estimate_metadata: Option<EstimateMetadata>) {
        log::info!("Calculating compressed size and time required for algorithm {:?} (workload \"{}\") (estimating: {})", self, workload.name, estimate_metadata.is_some());
        let (compressed_size, time_required) = match estimate_metadata {
            Some(metadata) => {
                let mut average_compressed_size = 0;
                let mut average_time_required = 0.;
                let current_unix = Instant::now();
                log::debug!("Estimating metrics by using {} blocks of ratio {}", metadata.block_number, metadata.block_ratio);
                for _ in 0..metadata.block_number {
                    let workload_size = workload.data.metadata().unwrap().len();
                    let block_size = (workload_size as f64 * metadata.block_ratio).round() as u64;
                    let block_end_index = rand::thread_rng().gen_range(block_size..workload_size);
                    let current_unix = Instant::now();
                    let block_compressed_size = self.execute_on_tmp(workload, Some(BlockInfo{ block_size, block_end_index })).metadata().unwrap().len();
                    let time = current_unix.elapsed().as_secs_f64();
                    average_time_required += time;
                    average_compressed_size += block_compressed_size;
                }
                average_compressed_size = ((average_compressed_size as f64 / metadata.block_number as f64) * (1./metadata.block_ratio).round()) as u64;
                average_time_required = (average_time_required / metadata.block_number as f64) * (1./metadata.block_ratio);
                log::debug!("Final metrics:\nCompressed size: {}\nTime required: {}\nTime taken for estimation: {:?}", average_compressed_size, average_time_required, current_unix.elapsed());
                (average_compressed_size, Duration::from_secs_f64(average_time_required))
            },
            None => {
                let current_unix = Instant::now();
                let result = self.execute_on_tmp(workload, None).metadata().unwrap().len();
                (result, current_unix.elapsed())
            }
        };
        log::info!("Compressed size and time required calculated for algorithm {:?}:\nCompressed size: {:?};\nTime required: {:?}", self, compressed_size as ByteSize, time_required);
        self.compressed_size = Some(compressed_size as ByteSize);
        self.time_required = Some(time_required);
    }
}
impl Algorithm for Gzip {
    fn name(&self) -> String {
        format!("Gzip_{}", self.compression_level.0)
    }

    fn compressed_size(&self) -> ByteSize {
        self.compressed_size.unwrap()
    }

    fn time_required(&self) -> Duration {
        self.time_required.unwrap()
    }

    fn execute(&self, w: &mut Workload) {
        let instant = Instant::now();
        log::debug!("Execute: init {:?}", instant.elapsed());
        let mut e = GzEncoder::new(&mut w.result_file, Compression::new(self.compression_level.0));
        log::debug!("Execute: encoder created {:?}", instant.elapsed());
        let mut pos = 0usize;
        let data_len = w.data.metadata().unwrap().len() as usize;
        while pos < data_len {
            let buffer_len = min(10_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
            log::debug!("Execute: written {} bytes so far (time: {:?})", pos, instant.elapsed());
        }
        log::debug!("Execute: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute: finished {:?}", instant.elapsed());
        w.data.rewind().unwrap();
    }

    fn execute_on_tmp(&self, w: &mut Workload, block_info: Option<BlockInfo>) -> File {
        let instant = Instant::now();
        log::debug!("Execute on tmp: init {:?}", instant.elapsed());
        let tmpfile = tempfile().unwrap();
        let mut e = GzEncoder::new(&tmpfile, Compression::new(self.compression_level.0));
        log::debug!("Execute on tmp: encoder created {:?}", instant.elapsed());
        let block_info = block_info.unwrap_or(BlockInfo{block_size: w.data.metadata().unwrap().len(), block_end_index: w.data.metadata().unwrap().len()});
        let mut start = block_info.block_end_index - block_info.block_size;
        let data_len = block_info.block_end_index;
        while start < data_len {
            let buffer_len = min(10_000_000, data_len - start);
            let mut buffer: Vec<u8> = vec![0; buffer_len as usize];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            start += buffer_len;
            log::debug!("Execute on tmp: written {} bytes so far (time: {:?})", start, instant.elapsed());
        }
        log::debug!("Execute on tmp: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute on tmp: finished {:?}", instant.elapsed());
        w.data.rewind().unwrap();
        tmpfile
    }

    fn execute_with_target(&self, w: &mut Workload, partition: usize, first_half: bool) {
        let instant = Instant::now();
        log::debug!("Execute with target: init {:?}", instant.elapsed());
        let mut e = GzEncoder::new(&w.result_file, Compression::new(self.compression_level.0));
        log::debug!("Execute with target: encoder created {:?}", instant.elapsed());
        let (mut pos, data_len) = if first_half {
            (0usize, partition)
        } else {
            (partition, w.data.metadata().unwrap().len() as usize)
        };
        if !first_half {
            w.data.seek(SeekFrom::Start(partition as u64)).expect("Partition is wrong");
        }
        while pos < data_len {
            let buffer_len = min(1_000_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut *buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
            log::debug!("Execute with target: written {} bytes so far (time: {:?})", pos, instant.elapsed());
        }
        log::debug!("Execute with target: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute with target: finished {:?}", instant.elapsed());
        w.data.rewind().unwrap();
    }
}
#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};
    use std::time::Duration;
    use tempfile::tempfile;
    use crate::algorithms::Algorithm;
    use crate::algorithms::gzip::{GzipCompressionLevel, Gzip};
    use crate::workload::Workload;

    const MOCK_WORKLOAD_DATA: &str = r#"
Nel mezzo del cammin di nostra vita
mi ritrovai per una selva oscura,
ché la diritta via era smarrita.

Ahi quanto a dir qual era è cosa dura
esta selva selvaggia e aspra e forte
che nel pensier rinova la paura!

Tant’è amara che poco è più morte;
ma per trattar del ben ch’i’ vi trovai,
dirò de l’altre cose ch’i’ v’ ho scorte.

Io non so ben ridir com’i’ v’intrai,
tant’era pien di sonno a quel punto
che la verace via abbandonai.

Ma poi ch’i’ fui al piè d’un colle giunto,
là dove terminava quella valle
che m’avea di paura il cor compunto,

guardai in alto e vidi le sue spalle
vestite già de’ raggi del pianeta
che mena dritto altrui per ogne calle.

Allor fu la paura un poco queta,
che nel lago del cor m’era durata
la notte ch’i’ passai con tanta pieta.

E come quei che con lena affannata,
uscito fuor del pelago a la riva,
si volge a l’acqua perigliosa e guata,

così l’animo mio, ch’ancor fuggiva,
si volse a retro a rimirar lo passo
che non lasciò già mai persona viva.

Poi ch’èi posato un poco il corpo lasso,
ripresi via per la piaggia diserta,
sì che ’l piè fermo sempre era ’l più basso.

Ed ecco, quasi al cominciar de l’erta,
una lonza leggera e presta molto,
che di pel macolato era coverta;

e non mi si partia dinanzi al volto,
anzi ’mpediva tanto il mio cammino,
ch’i’ fui per ritornar più volte vòlto.

Temp’era dal principio del mattino,
e ’l sol montava ’n sù con quelle stelle
ch’eran con lui quando l’amor divino

mosse di prima quelle cose belle;
sì ch’a bene sperar m’era cagione
di quella fiera a la gaetta pelle

l’ora del tempo e la dolce stagione;
ma non sì che paura non mi desse
la vista che m’apparve d’un leone.

Questi parea che contra me venisse
con la test’alta e con rabbiosa fame,
sì che parea che l’aere ne tremesse.

Ed una lupa, che di tutte brame
sembiava carca ne la sua magrezza,
e molte genti fé già viver grame,

questa mi porse tanto di gravezza
con la paura ch’uscia di sua vista,
ch’io perdei la speranza de l’altezza.

E qual è quei che volontieri acquista,
e giugne ’l tempo che perder lo face,
che ’n tutti suoi pensier piange e s’attrista;

tal mi fece la bestia sanza pace,
che, venendomi ’ncontro, a poco a poco
mi ripigneva là dove ’l sol tace.

Mentre ch’i’ rovinava in basso loco,
dinanzi a li occhi mi si fu offerto
chi per lungo silenzio parea fioco.

Quando vidi costui nel gran diserto,
"Miserere di me", gridai a lui,
"qual che tu sii, od ombra od omo certo!".

Rispuosemi: "Non omo, omo già fui,
e li parenti miei furon lombardi,
mantoani per patrïa ambedui.

Nacqui sub Iulio, ancor che fosse tardi,
e vissi a Roma sotto ’l buono Augusto
nel tempo de li dèi falsi e bugiardi.

Poeta fui, e cantai di quel giusto
figliuol d’Anchise che venne di Troia,
poi che ’l superbo Ilïón fu combusto.

Ma tu perché ritorni a tanta noia?
perché non sali il dilettoso monte
ch’è principio e cagion di tutta gioia?".

"Or se’ tu quel Virgilio e quella fonte
che spandi di parlar sì largo fiume?",
rispuos’io lui con vergognosa fronte.

"O de li altri poeti onore e lume,
vagliami ’l lungo studio e ’l grande amore
che m’ ha fatto cercar lo tuo volume.

Tu se’ lo mio maestro e ’l mio autore,
tu se’ solo colui da cu’ io tolsi
lo bello stilo che m’ ha fatto onore.

Vedi la bestia per cu’ io mi volsi;
aiutami da lei, famoso saggio,
ch’ella mi fa tremar le vene e i polsi".

"A te convien tenere altro vïaggio",
rispuose, poi che lagrimar mi vide,
"se vuo’ campar d’esto loco selvaggio;

ché questa bestia, per la qual tu gride,
non lascia altrui passar per la sua via,
ma tanto lo ’mpedisce che l’uccide;

e ha natura sì malvagia e ria,
che mai non empie la bramosa voglia,
e dopo ’l pasto ha più fame che pria.

Molti son li animali a cui s’ammoglia,
e più saranno ancora, infin che ’l veltro
verrà, che la farà morir con doglia.

Questi non ciberà terra né peltro,
ma sapïenza, amore e virtute,
e sua nazion sarà tra feltro e feltro.

Di quella umile Italia fia salute
per cui morì la vergine Cammilla,
Eurialo e Turno e Niso di ferute.

Questi la caccerà per ogne villa,
fin che l’avrà rimessa ne lo ’nferno,
là onde ’nvidia prima dipartilla.

Ond’io per lo tuo me’ penso e discerno
che tu mi segui, e io sarò tua guida,
e trarrotti di qui per loco etterno;

ove udirai le disperate strida,
vedrai li antichi spiriti dolenti,
ch’a la seconda morte ciascun grida;

e vederai color che son contenti
nel foco, perché speran di venire
quando che sia a le beate genti.

A le quai poi se tu vorrai salire,
anima fia a ciò più di me degna:
con lei ti lascerò nel mio partire;

ché quello imperador che là sù regna,
perch’i’ fu’ ribellante a la sua legge,
non vuol che ’n sua città per me si vegna.

In tutte parti impera e quivi regge;
quivi è la sua città e l’alto seggio:
oh felice colui cu’ ivi elegge!".

E io a lui: "Poeta, io ti richeggio
per quello Dio che tu non conoscesti,
acciò ch’io fugga questo male e peggio,

che tu mi meni là dov’or dicesti,
sì ch’io veggia la porta di san Pietro
e color cui tu fai cotanto mesti".

Allor si mosse, e io li tenni dietro."#;

    #[test]
    fn gzip() {
        let mut tmp = tempfile().unwrap();
        tmp.write_all(MOCK_WORKLOAD_DATA.as_bytes()).unwrap();
        tmp.rewind().unwrap();
        let mut workload = Workload::new(String::from("test"), tmp, Duration::from_secs(1));
        let alg = Gzip::new(&mut workload, GzipCompressionLevel(9), None);
        alg.execute(&mut workload);
        println!("Time: {:?}", alg.time_required());
        assert_eq!(workload.data.metadata().unwrap().len(), 5265);
        assert_eq!(alg.compressed_size(), 2529);
    }
}