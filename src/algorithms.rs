pub mod gzip;
pub mod bzip2;
pub mod xz2;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::io::Cursor;
use std::time::Duration;
use crate::convex_hull::Point;
use crate::workload::Workload;

pub type ByteSize = u64;

/// Defines compression algorithms
pub trait Algorithm: Debug {
    fn name(&self) -> String;
    /// Estimates the compressed size obtained by running this algorithm on workload w.
    fn compressed_size(&mut self, w: &Workload) -> ByteSize;
    /// Estimates the time budget required to execute this algorithm on workload w.
    fn time_required(&mut self, w: &Workload) -> Duration;
    /// Runs the compression algorithm on some workload.
    fn execute(&self, w: &Workload) -> Vec<u8>;

    /// Runs the compression algorithm on some workload, by writing on a cursor target to optimize memory writes.
    fn execute_with_target(&self, w: &Workload, target: &mut Cursor<Vec<u8>>);
}


// Specifies metrics related to a specific algorithm ran on a specific workload.
#[derive(Debug)]
pub struct AlgorithmMetrics {
    pub compressed_size: ByteSize,
    pub time_required: Duration,
    pub algorithm: Box<dyn Algorithm>,
}

impl AlgorithmMetrics {
    pub fn new(mut algorithm: Box<dyn Algorithm>, workload: &Workload) -> AlgorithmMetrics {
        AlgorithmMetrics {
            compressed_size: algorithm.compressed_size(workload),
            time_required: algorithm.time_required(workload),
            algorithm,
        }
    }
}

impl PartialOrd for AlgorithmMetrics {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.time_required == other.time_required {
            // Secondary index, inverse (smaller is better)
            return other.compressed_size.partial_cmp(&self.compressed_size);
        }
        self.time_required.partial_cmp(&other.time_required)
    }
}

impl Ord for AlgorithmMetrics {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for AlgorithmMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.compressed_size == other.compressed_size && self.time_required == other.time_required
    }
}

impl Eq for AlgorithmMetrics {}

impl Point for AlgorithmMetrics {
    fn x(&self) -> f64 {
        self.time_required.as_secs_f64()
    }

    fn y(&self) -> f64 {
        self.compressed_size as f64
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
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
        let mut alg = Gzip::new(GzipCompressionLevel(9));
        let workload = Workload::new(MOCK_WORKLOAD_DATA.as_bytes(), Duration::from_secs(1));
        alg.execute(&workload);
        println!("{:?} - {:?} - {:?}", workload.data.len(), alg.compressed_size(&workload), alg.time_required(&workload));
    }
}