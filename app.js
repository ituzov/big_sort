const fs = require('fs');
const readline = require('readline');
const { Transform } = require('stream');

const CHUNK_SIZE = 100000; // Размер чанка в строках
const INPUT_FILE = 'bigfile.txt'; // Исходный файл
const OUTPUT_FILE = 'sorted.txt'; // Результат
const TEMP_DIR = './temp'; // Директория для временных файлов

// Создание директории для временных файлов
if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR);
}

// Функция разбиения большого файла на части
async function splitFile() {
    const fileStream = fs.createReadStream(INPUT_FILE);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let count = 0;
    let part = 0;
    let data = [];
    for await (const line of rl) {
        data.push(line);
        count++;
        if (count >= CHUNK_SIZE) {
            data.sort();
            fs.writeFileSync(`${TEMP_DIR}/part${part}.txt`, data.join('\n'));
            data = [];
            count = 0;
            part++;
        }
    }

    if (data.length > 0) {
        data.sort();
        fs.writeFileSync(`${TEMP_DIR}/part${part}.txt`, data.join('\n'));
    }
}

// Функция слияния файлов
async function mergeFiles() {
    const files = fs.readdirSync(TEMP_DIR).map(file => `${TEMP_DIR}/${file}`);
    const streams = files.map(file => readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity }));
    const iterators = streams.map(stream => stream[Symbol.asyncIterator]());
    const nextLines = await Promise.all(iterators.map(iterator => iterator.next()));

    const sortedStream = new Transform({
        writableObjectMode: true,
        transform(chunk, encoding, callback) {
            this.push(chunk.value + '\n');
            callback();
        }
    });

    sortedStream.pipe(fs.createWriteStream(OUTPUT_FILE));

    while (true) {
        const validLines = nextLines.filter(line => !line.done);
        if (validLines.length === 0) break;

        const minLine = validLines.reduce((min, current) => current.value < min.value ? current : min);
        const minIndex = nextLines.indexOf(minLine);

        sortedStream.write(minLine);
        nextLines[minIndex] = await iterators[minIndex].next();
    }

    sortedStream.end();
}

// Очистка временных файлов
function cleanup() {
    const files = fs.readdirSync(TEMP_DIR);
    for (const file of files) {
        fs.unlinkSync(`${TEMP_DIR}/${file}`);
    }
    fs.rmdirSync(TEMP_DIR);
}

// Запуск
splitFile()
    .then(() => mergeFiles())
    .then(() => cleanup())
    .catch(err => {
        console.error(err);
        cleanup();
    });
