
const N = 500
let soFar = 0
let books = []
let urls = []

function handlePage(data) {
	const res = data["results"]
	// Sort in descending order download count
	res.sort((o1, o2) => (o2["download_count"] - o1["download_count"]))
	
	const txtFormat = "text/plain; charset=utf-8"
	const topN = res.filter(o => 
		o["formats"][txtFormat] && !o["formats"][txtFormat].includes(".zip")
	).slice(0, N - soFar)

	soFar += topN.length

	books.push(...topN.map(o => o["title"]))
	urls.push(...topN.map(o => o["formats"][txtFormat]))

	if(soFar < N) {
		console.log(`(${soFar}/${N})`)
		return fetch(data["next"]).then(resp => resp.json()).then(handlePage)
	} else {
		console.log(`Top ${N} books`)
		console.dir(books, {'maxArrayLength': null})
		console.dir(urls, {'maxArrayLength': null})

		console.log(`Last download count: ${topN.pop()["download_count"]}`)
	}
}

async function main() {
	await fetch("http://gutendex.com/books?sort=popular&languages=en&mime_type=text%2F")
		.then(resp => resp.json())
		.then(handlePage)
}

main()
