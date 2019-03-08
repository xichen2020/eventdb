package fs

import (
	"testing"

	docfield "github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/stretchr/testify/assert"
)

// BenchmarkReadField writes a segment with 4 million strings to disk (10MB) and then
// reads the segment from disk and iterates over the strings one by one.
// If the segment file is NOT present in the system file cache, the initial loading of
// the file takes 20ms more than if the file is already present in the cache.
func BenchmarkReadField(b *testing.B) {
	var (
		totDocs     = 1024 * 1024 * 80
		totRand     = len(randomBytes)
		namespace   = []byte("namespace")
		fieldPath   = []string{"foo.bar"}
		segMetadata = segment.Metadata{
			ID:           "sa8dj32h9sd",
			MinTimeNanos: 100000000,
			MaxTimeNanos: 100001000,
		}
		opts       = NewOptions().SetFilePathPrefix("/tmp/")
		fieldTypes = docfield.ValueTypeSet{
			docfield.BytesType: struct{}{},
		}
		numDocs int32
	)
	writer := newSegmentWriter(opts)
	builder := field.NewDocsFieldBuilder(fieldPath, nil)

	for i := 0; i < totDocs; i++ {
		if i%2 == 0 {
			builder.Add(int32(i), docfield.NewBytesUnion(iterator.Bytes{
				Data: []byte(randomBytes[i%totRand]),
			}))
			numDocs++
		}
	}
	segMetadata.NumDocs = numDocs
	docsField := builder.Seal(int32(totDocs))
	err := writer.Start(writerStartOptions{
		Namespace:   namespace,
		SegmentMeta: segMetadata,
	})
	assert.NoError(b, err)
	assert.NoError(b, writer.WriteFields([]indexfield.DocsField{docsField}))

	reader := newSegmentReader(namespace, opts)
	reader.Open(readerOpenOptions{SegmentMeta: segMetadata})

	df, err := reader.ReadField(persist.RetrieveFieldOptions{FieldPath: fieldPath, FieldTypes: fieldTypes})
	assert.NoError(b, err)

	sf, ok := df.BytesField()
	assert.True(b, ok)
	iter, err := sf.Iter()
	assert.NoError(b, err)
	for iter.Next() {
		_ = iter.Value()
	}
}

var (
	randomBytes = []string{
		"F8MCaDITND",
		"tymDDCKxzJ",
		"mvgWvjecnH",
		"egvHQLqFCN",
		"MONuJcF7GZ",
		"0Tjc7RxB6n",
		"gXdewI4o7Y",
		"cqiE5cyYn3",
		"Ov4s7BP1mM",
		"h7Qom1bvbh",
		"tjaE1XQqbv",
		"t0YMzkaxTA",
		"VAGT2ZnooR",
		"Qve3EAsR59",
		"vHUdd7vhDu",
		"zLAy3nhd38",
		"nqBLbUysDk",
		"55f5vmcOph",
		"HOQFTDBQoT",
		"75vKUbfYky",
		"MFGJXiUP0Y",
		"Ne4dIL4Z7i",
		"eAW90S2Sz0",
		"d6t9766saA",
		"g9IpqGzeEE",
		"AOG48ZzFWG",
		"NoBxPbciZM",
		"x9s89ZYIO0",
		"TZJqtvmpVT",
		"XeQh2T93Ng",
		"XyRGHxVXJe",
		"6vjIgL1j9o",
		"4hMPjXiJIg",
		"krgsd5w64P",
		"CbKyragtuq",
		"UtRv9ysU7q",
		"rYilDvcy64",
		"R3Xv6sfkY7",
		"xNrF8jCqFQ",
		"q9cpXvrP7y",
		"iHTuYGuop6",
		"hUaPHVxjoG",
		"NAQshJkFtJ",
		"ooOxQEO98x",
		"BYjhaxVwi1",
		"dMnLrUyCO5",
		"0ibrA1nMOe",
		"Qbi0spelpr",
		"Xj1wx5jeYc",
		"uAqG6sG1hl",
		"m250lgtJR5",
		"y7cYnNRxAy",
		"SbqAEBK4uH",
		"TcCWvIQdNo",
		"QShyMQodUg",
		"hBFm7bhDq5",
		"CeLekTer36",
		"H1hzfYDwDL",
		"98Bo8Bpoxj",
		"KqmiozOcnu",
		"5EQPrYsX2O",
		"1vjtbt2ObT",
		"276vCwjgL9",
		"nX9TurhqzT",
		"Wnuv8p1IIA",
		"s0HohM6Ffx",
		"vOb2zcjjQt",
		"FBDVkqiBqo",
		"w8j7Wah6L3",
		"cCLmKAbJSx",
		"qLFNgbfRzl",
		"uv2DMAnrqe",
		"P2V1w2w6u0",
		"lipBAvayx1",
		"41EnNtAeD0",
		"Du1fE8982Z",
		"bBuHwvGUQH",
		"1QkhQ4xIEH",
		"6S925nz6Ig",
		"cCFqYnURbE",
		"EqWl9pKppS",
		"SxJZ3STI9t",
		"83HGB47xPg",
		"tAMFhtGkxD",
		"uhiYgFCDHZ",
		"eieL9Tp8IY",
		"FP0o1crNln",
		"39G060pUTP",
		"7PEYV1igKR",
		"1y4ay7RC3i",
		"CuYJXmwua9",
		"WRvlelWDTq",
		"0Ym9HFtptv",
		"onc7XryTAK",
		"x5YGkeeoNL",
		"1ws91Zkf0p",
		"llQhs0slML",
		"w9Qhzbarfy",
		"WuBzLh69zD",
		"TFASK7kyFH",
		"yDPtFpept5",
		"sSar0mjX64",
		"0d4yLbFTDp",
		"dgVETMJJ1j",
		"v5JnTmBI0B",
		"4YD7SIUw6L",
		"UPOLX8IfkY",
		"CUWLYVvXCv",
		"BxgbcBPPzN",
		"zEfgwvm9Fa",
		"1MDUvonM2L",
		"TvpZy2oc3n",
		"eAxg2jXh4K",
		"qaZ8Sk90EP",
		"7S9oACVeM5",
		"nkiPn5xHhD",
		"uCbDVpY4j2",
		"kjHx3TbZaV",
		"9Fn9jnrmEt",
		"fi834E2NCG",
		"Fyss7KIHqV",
		"voOEoYL7Fh",
		"Wf5MON7lNG",
		"v5R3IcGeqj",
		"NxkCgP0c1F",
		"Juw4NaHjW6",
		"111Ktyd1Gc",
		"mBOdtjbAy3",
		"fVOWrw13ce",
		"7n4ecNRd31",
		"pZosqf6kHr",
		"DpCzPMRhUU",
		"fDIDYSbhof",
		"NurwyBNQ3j",
		"YktiUO1NSu",
		"EfDK3PTJiO",
		"r9yyvCd3py",
		"wr9dqtKJ6L",
		"JkVI6ZHfJP",
		"HQhNvzLksd",
		"FQ54M5XZTp",
		"2002pWz3dQ",
		"WymCisn5R3",
		"VeyWXKrwJL",
		"8q5gIK1gP6",
		"GNf1940xWm",
		"IChO5botTq",
		"9YyCcRT4PE",
		"jyEywZBc0j",
		"8gLIXyE8JY",
		"J3UiB9qlXP",
		"ntlxmZPulB",
		"r9mxNXMQqn",
		"ylB2UP7fss",
		"sOzu5s5ROA",
		"iz6W5HfVlz",
		"3iEQeD8oRR",
		"H7KOkZy6I6",
		"ZulEoUcpAX",
		"O0YvbOi71I",
		"qNjLYWz2uO",
		"D7PY9toWXl",
		"rLcpkRofOk",
		"xTf2zBV9Oy",
		"dWr4KzWVYV",
		"n62NiQGe7U",
		"tEzWXqOUn1",
		"Q6QScgbbUQ",
		"5IM38DTdF0",
		"zUyNZKHxFm",
		"TH1cTAtiHU",
		"JAplakSGlP",
		"XtdPJydP0H",
		"eqn7qpiLPX",
		"2I7O90QgBr",
		"2O9VisIbgw",
		"z18yPrUint",
		"0xOnmbpymF",
		"eLobu6ZDod",
		"evNVQOLuYc",
		"X22TRn66CA",
		"9Wu794iLZb",
		"okR9Jjxq6Q",
		"3l7NGRet2m",
		"YKuSNz4Dic",
		"UnEOvv4EYo",
		"nS3S4PNn11",
		"jxqqHgigrx",
		"rfeR9sjPNv",
		"AhHZPZTf9G",
		"WKSHmHTQCD",
		"e9PFscuo2F",
		"yUkssvEmOb",
		"BUH06HMF6a",
		"LIVhbKGac0",
		"xemWT5UYjz",
		"d1EzW1R0sS",
		"Fnd31ZlVkq",
		"QQ8qfLk0Xz",
		"fGtx0jOJFg",
		"G9hUeUW7xs",
		"vK93wApSRD",
		"H4QBh0iHiy",
		"nbP83UMpHn",
		"VNi1sqfmQ5",
		"eWZZK4Ts1P",
		"yJo8ZaC1RZ",
		"8YqQgwjA94",
		"mvaMXGLlcu",
		"65zGxd4xpV",
		"gcG8uvZxml",
		"r4V6lrirCJ",
		"Lz9bByBCee",
		"XUSr1AkD8E",
		"nyv1sbkSnh",
		"oaPvxSrbnp",
		"Bd7rX7zEFI",
		"UNdegy1T5g",
		"lOUL7YoiOM",
		"IFPBtWVvc6",
		"w7mKGyWFck",
		"JwvDokejEG",
		"WgsjOyv8KX",
		"i8ZdiGQqHY",
		"SM28wANgNo",
		"2vdgQ2nAbN",
		"pf1pMDzJRs",
		"MkbpeU1CT6",
		"kfbPkw2Zxo",
		"97uEjQPTvz",
		"yzs12HDNR8",
		"GgJPYQLOdt",
		"g9kXaaUrou",
		"MpfTUBQ5lB",
		"geOEShVYC0",
		"MNysSOgB39",
		"i0pkwgp24u",
		"JlcpOElJfa",
		"W7wnEEjeBG",
		"bWPW8zK8SZ",
		"6ZL1ZcmECK",
		"ybkjVWdIJJ",
		"mcbzj5AJwL",
		"FbTM2qo7Wl",
		"KE0bt3hcz2",
		"LEVr17yqwB",
		"mDZxphMtaK",
		"yNzJ79EMAK",
		"tanpLZfQqD",
		"ZM6GQpIqcS",
		"OYpMJYxhdW",
		"roPrE2z5ff",
		"TPXqQHUHNj",
		"jpwBvY6zF8",
		"40Msir38Bo",
		"wtIfqf6IUY",
		"9HcQ2KM39Y",
		"Kg6pasuCQU",
		"HriUUHFIz4",
		"R3ZFDwPHnK",
		"02REDbYzb6",
		"EiVYihmkTd",
		"DqDRdYBv1N",
		"ID3iRKV2J2",
		"Jr6HT7IqoK",
		"CPJjCgEWuR",
		"iwd9CslEbC",
		"IoxlkHRY25",
		"hOwnOfaH9u",
		"A5ZWIXfs2r",
		"oqKKxDvjmq",
		"kqQn8K2CEU",
		"ge6CCgoYD6",
		"UMClYm0NF9",
		"6MpxXStnwm",
		"2p26yTgD5s",
		"rVAzSkl6F7",
		"8fc56iHABx",
		"Lg3rWWCe6B",
		"IUEgwhrfSe",
		"EhSMFbYeDk",
		"QHKGKUwAUe",
		"n8Q4MmKtPP",
		"BV5RtDO2PC",
		"LTEhG2TPTE",
		"K6xJkNiusq",
		"cYRtL5mGVj",
		"FOixqAGfQB",
		"54DouI4Jnr",
		"U5TmhMphVI",
		"2OtQAziivE",
		"hKI80BYofy",
		"DPOBDqGrZu",
		"pN3cE4KrRE",
		"H3S8yyCvsi",
		"EZZY2PSmV1",
		"dwlcGmOOx8",
		"xFQ5LiOP5r",
		"l9SuMSaX9A",
		"McZO9fTXkn",
		"DsyyRTOEcO",
		"3KpVhgGd8O",
		"DeDyUZJCse",
		"lhlAIWdOKc",
		"spH7VsOS7o",
		"oobTUUrPlf",
		"PJRfax5fnp",
		"frWGMUvUaI",
		"fu2cDOY3cp",
		"Sw765W7LdL",
		"Pp6AapMHmX",
		"rqF6c8FcIh",
		"fgcFGxbNPu",
		"tmXRuhhunZ",
		"rxAeJCgh50",
		"TKT5UKG8Lt",
		"lmOgH4v5SM",
		"zBoGkLEKzH",
		"qw4jsD41lq",
		"DAVpxjKXeh",
		"sKu9SmsLJP",
		"D7luGI4F0o",
		"A95va1Vhoy",
		"l9I61LNZ7u",
		"JwpZlZUIAm",
		"ecziZTbIM3",
		"eoEwSxsnnI",
		"uvydcvI0dg",
		"5ieOpb8m9r",
		"C90FCKi3ob",
		"4WJw5vBXdC",
		"7vgj33XBTy",
		"NBm4r8o4Pf",
		"RsXXiNuZfv",
		"Av8CrmVt2p",
		"tJBha7wCMV",
		"MdISBno3oM",
		"O8rezskNuN",
		"KRDRATK1Vj",
		"P7OZqP9pRH",
		"K1kuPEyWKF",
		"Myu1D99NeO",
		"g10n91LIBK",
		"pBvMrQzsAD",
		"dQN2t33GuW",
		"T552xxUUHv",
		"PzYAixY1NH",
		"H5GbD1L9hQ",
		"Qsdjuz2Aqn",
		"wh634NFG6T",
		"5meE8x1BFp",
		"YvtbqCKYVu",
		"Npp0ZudKlB",
		"VhPT8Bwgjb",
		"WLW1VOEPNH",
		"osBVUOlFdz",
		"U8yna7cfmT",
		"sdkq0z8T5b",
		"suN0HNV8LE",
		"W3b4PqGrEH",
		"B8W2zeihs9",
		"S9uZaBZ012",
		"7IszwFtCnM",
		"VcEOUMzfyY",
		"5Qu21p6M87",
		"MImrCsq5Oa",
		"jcXFjONQtU",
		"qIJ1d3VdpY",
		"nOHCOb46F8",
		"q7fI8yVP0B",
		"9hlT7mZVaW",
		"PfXnCbxsx7",
		"ovWE51t8SQ",
		"tYEuYojfiT",
		"Z1kxnd5t0C",
		"iqsHapNRph",
		"uJrrJuXJZk",
		"0qRszul3Xj",
		"zKC3yaMP98",
		"6IXHVqTjiN",
		"hP2UdEYYdm",
		"r6kbGEiml5",
		"MCvhyosCDW",
		"bk3BuHSRdV",
		"a2XOj1mQOb",
		"TldWRVW9dC",
		"WBVSVHFYfZ",
		"DzXoVunQMt",
		"O9gFOdfDsa",
		"9Ubqc0ZktS",
		"RQlSC53Vid",
		"IT74gqeenM",
		"D81bScrXn0",
		"HvHQEw9ypN",
		"vLbkYzHTWw",
		"Ua56Ai3rXc",
		"exXgGqGJmR",
		"zPUH85gxZI",
		"qtq475OnsL",
		"O00zJdr4pI",
		"a3FeuaLjvb",
		"Ht2rANYjSA",
		"EBE2SzYo38",
		"ldrXtaRV2r",
		"eBgUKVe5Gs",
		"F1pLLgVsaM",
		"kSaWMB4Nrc",
		"XQazwTjQcS",
		"oEB7lwvSk2",
		"7K6Qn8m6Z2",
		"7WJOb2OJme",
		"z7Vsm0U7J4",
		"rNKr8QBCcs",
		"nWFhODaLel",
		"9LGqFL2YoC",
		"Z41XdWPpfK",
		"dVOYGt71YA",
		"Ac7AmuiuXU",
		"WU0mTGX7o0",
		"cEh3C7bAoU",
		"fS62k8OGdg",
		"31p2wrdrJm",
		"hVYO4SsFn1",
		"yoNjdNVR3T",
		"nXGB72ybp2",
		"cHQZZgB11M",
		"Ffnd1A90JC",
		"8nXDQJ0ezP",
		"UpDWJvSzR2",
		"JacpsIOfzj",
		"Lzf26s7HGi",
		"g66PTqXaRy",
		"TPb0YxW52c",
		"d1U8peIXDy",
		"N6QCOI1usv",
		"I6n9xaKB9q",
		"9NU8EDDqSN",
		"zFIPc1u4Pl",
		"nqTm3rPf96",
		"84oTCP6YcY",
		"YWS7VasgJU",
		"NoMN2CiKGW",
		"4dTellGN0o",
		"ypR0MNTLFT",
		"hmfstceE9I",
		"WwulsxgWWn",
		"W7kagdUve6",
		"IcnJMAVR0g",
		"oVkxCcqGWm",
		"9zix1C9wIy",
		"ypbcixxmh2",
		"5ttv3zVGmV",
		"G2bPRcDfrY",
		"IJUCJSd0fI",
		"9fRiclzavV",
		"WnKAPECQ8D",
		"nqcyYI8Ok3",
		"ZGn28e4uRL",
		"yUwL3FYMM7",
		"upDrfj18nN",
		"ofktxu9EOy",
		"8mXY3bYvSJ",
		"DzpPPRcohB",
		"bG1hCLtlTI",
		"0IUVoPLTje",
		"xNZdHKCkNL",
		"YcBR2SFlkF",
		"2B2jQUe3eo",
		"JqQigL7L9N",
		"IpaVKZ4itN",
		"EuDA3aIrRX",
		"M1fjFiTxin",
		"0Y3uLIAv4Q",
		"pL1cP0BAmT",
		"5j9gKqS6bC",
		"IXLOVGc296",
		"WtUPod1O2Y",
		"Fo137BTGKE",
		"vDNrexb5pY",
		"Z87QLTu8sn",
		"4NnepdyQIl",
		"wCncG1e6Rd",
		"6XyHB8Z0ah",
		"pk7Zs4whvT",
		"eOgHSRWF9G",
		"t9jZW2NZwo",
		"OcfB8QIOfc",
		"jkfHphAWxD",
		"290y9LChSf",
		"xGNN8Xl4S3",
		"mzjHvx6w0R",
		"h9GCmJUL2u",
		"k2O2dNPV9l",
		"mcg1GAzWNc",
		"9kfK5B7CyA",
		"DwILNaIn2x",
		"QdiPnCE7yu",
		"dE5Tg1YWcG",
		"kBcZ2GfuWd",
		"nAYfDOmfaO",
	}
)
