use crossbeam_queue::SegQueue;
use scraper::{Html, Selector};
use std::time::Duration;
use std::vec;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Semaphore;
use url::Url;

const MAX_CONCURRENT_CRAWL_TASKS: usize = 100;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let seed_url = std::env::args()
        .nth(1)
        .expect("required to pass a (single) seed url as first argument. e.g. cargo run -- \"https://axiom.co/\"");
    let seed_urls = vec![seed_url];
    start_crawler(seed_urls).await?;
    Ok(())
}

async fn start_crawler(seed_urls: Vec<String>) -> anyhow::Result<()> {
    println!("Starting crawler");

    let mut visited_urls: HashSet<String> = HashSet::new();
    let crawl_task_limit_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CRAWL_TASKS));
    let urls_to_crawl = Arc::new(SegQueue::<String>::new());
    for url in seed_urls {
        urls_to_crawl.push(url);
    }

    loop {
        let urls_to_crawl = Arc::clone(&urls_to_crawl);

        match urls_to_crawl.pop() {
            None => {
                // instead of keeping track of pending tasks explicitly via a Vec<JoinHandle> or similar, we use the
                // semaphore to check to indirectly there are still pending tasks.
                // This mainly to avoid an ever-growing Vec<JoinHandle>, which would consume a good amoutn of increasing memory.
                // There might be better / more idiomatic ways to do this, but appears to work well enough, without consuming too much memory.
                if crawl_task_limit_semaphore.available_permits() == MAX_CONCURRENT_CRAWL_TASKS {
                    // no more work to do or pending. we can exit the loop (and program) here.
                    break;
                } else {
                    // currently no more new urls, but there are still pending tasks, so let's poll the queue again after a short sleep.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            Some(url) => {
                if !visited_urls.insert(url.clone()) {
                    // we've already visited this url, so we can skip it.
                    continue;
                }
                let permit = Arc::clone(&crawl_task_limit_semaphore)
                    .acquire_owned()
                    .await;
                tokio::spawn(async move {
                    match crawl(url.clone()).await {
                        Ok(urls_found) => {
                            for url in urls_found {
                                urls_to_crawl.push(url);
                            }
                        }
                        Err(e) => {
                            println!("Error crawling url: {} ,error: {}", url, e);
                        }
                    }
                    drop(permit);
                });
            }
        }
    }

    println!("Crawler finished. Crawled {} urls", visited_urls.len());

    return Ok(());
}

async fn crawl(url: String) -> anyhow::Result<Vec<String>> {
    println!("Crawling: {}", url);

    let base_url = Url::parse(&url)?;
    let body = reqwest::Client::new()
        .get(&url)
        // timeout is quite important, as we otherwise hang virtually forever on some sites.
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    let body_html_fragment = Html::parse_fragment(body.as_str());
    let a_href_selector = Selector::parse("a[href]")
        .expect("Could not parse selector a[href]. This is a bug in our code.");

    let mut urls_found: Vec<String> = vec![];

    for a_href_element in body_html_fragment.select(&a_href_selector) {
        let href = a_href_element.attr("href").expect(
            "could not get href, even though href selector matched this element. This is likely a bug.",
        );
        let absolute_link = base_url.join(href)?;
        urls_found.push(absolute_link.to_string());
    }
    Ok(urls_found)
}
