use scraper::{Html, Selector};
use std::time::Duration;
use std::vec;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
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
type CrawlJobResult = anyhow::Result<Vec<String>>;

async fn start_crawler(seed_urls: Vec<String>) -> anyhow::Result<()> {
    println!("Starting crawler");

    let mut seen_urls: HashSet<String> = HashSet::new();
    let (crawl_job_results_tx, mut crawl_jobs_results_rx) =
        tokio::sync::mpsc::unbounded_channel::<CrawlJobResult>();
    let (urls_to_crawl_tx, urls_to_crawl_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    // we're using a counter to keep track of how many crawl jobs are still pending.
    // This is used to determine when there's nothing more left to crawl, at which point we stop the process gracefully.
    // Using this counter is a bit ugly/hacky and there's a probably a cleaner way to do this.
    let mut pending_crawl_jobs = 0;

    for url in seed_urls {
        pending_crawl_jobs += 1;
        urls_to_crawl_tx.send(url).unwrap();
    }

    tokio::spawn(async move {
        start_dispatcher(urls_to_crawl_rx, crawl_job_results_tx)
            .await
            .unwrap();
    });

    while let Some(crawl_job_result) = crawl_jobs_results_rx.recv().await {
        pending_crawl_jobs -= 1;

        if let CrawlJobResult::Ok(urls_found) = &crawl_job_result {
            for url in urls_found {
                if seen_urls.insert(url.clone()) {
                    // the insert returns true if the url was not already in the set, so we're only adding this to urls_to_crawl if we haven't seen it before.
                    pending_crawl_jobs += 1;
                    urls_to_crawl_tx.send(url.to_string()).unwrap();
                }
            }
        }

        if pending_crawl_jobs == 0 {
            break;
        }
    }

    println!("Crawler finished. Crawled {} urls", seen_urls.len());

    return Ok(());
}

async fn start_dispatcher(
    mut urls_to_crawl_rx: UnboundedReceiver<String>,
    crawl_results_tx: UnboundedSender<CrawlJobResult>,
) -> anyhow::Result<()> {
    let crawl_task_limit_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CRAWL_TASKS));

    while let Some(url) = urls_to_crawl_rx.recv().await {
        let tx = crawl_results_tx.clone();
        let permit = crawl_task_limit_semaphore.clone().acquire_owned().await;
        tokio::spawn(async move {
            match crawl(url.clone()).await {
                Ok(urls_found) => {
                    tx.send(CrawlJobResult::Ok(urls_found)).unwrap();
                }
                Err(e) => {
                    println!("Error crawling url: {} ,error: {}", url, e);
                    tx.send(CrawlJobResult::Err(e)).unwrap();
                }
            }
            drop(permit)
        });
    }

    Ok(())
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
