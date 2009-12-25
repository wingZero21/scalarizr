#!/usr/bin/php
<?php
# Connect to lib.ru and start reading books.

$urls = array(
	//"http://www.lib.ru/SELINGER/sel_1.txt",
	//"http://www.lib.ru/NLP/liri.txt",
	"http://www.lib.ru/KIZI/poroj.txt"
);

foreach ($urls as $url) {
	$fp = fopen($url, "r");
	while (!feof($fp)) {
		print fread($fp, 4096);
	}
	fclose($fp);
}

?>