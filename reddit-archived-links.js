const NUM_WIDTH = 12
const pad = (number) => String(number).padStart(NUM_WIDTH, '0')

const prefix = "https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-"

const getUrlFor = (n) => (
	prefix + pad(n) + ".csv.gzip"
)

const NUrls = 79

async function convertUrl(n) {
	const API = "https://archive.org/wayback/available?url="
	return fetch(API + getUrlFor(n))
		.then(resp => resp.json())
		.then(data => data["archived_snapshots"]["closest"]["url"])
		//.then(data => console.log(data["archived_snapshots"]["closest"]["url"]))
}

async function main() {
	let promises = []
	for(let i = 0; i < NUrls; i++) {
		promises.push(convertUrl(i))
	}
	await Promise.all(promises).then(console.log).catch(console.log)
}

main()
