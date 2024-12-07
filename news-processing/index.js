const https = require('https');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');

const ddbClient = new DynamoDBClient();
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const eventbridge = new EventBridgeClient();

exports.handler = async function() {
    console.log('Starting news ingestion');
    
    try {
        const newsApiKey = process.env.NEWS_API_KEY;
        if (!newsApiKey) {
            throw new Error('NEWS_API_KEY not configured');
        }

        const articles = await getNewsArticles(newsApiKey);
        console.log(`Retrieved ${articles.length} articles`);

        for (const article of articles) {
            await processArticle(article);
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Success',
                count: articles.length
            })
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Error processing news',
                error: error.message
            })
        };
    }
};

async function getNewsArticles(apiKey) {
    const options = {
        hostname: 'newsapi.org',
        path: '/v2/top-headlines?country=us',
        method: 'GET',
        headers: {
            'User-Agent': 'PRISM-News-Aggregator/1.0',
            'X-Api-Key': apiKey
        }
    };
    
    return new Promise((resolve, reject) => {
        const req = https.request(options, response => {
            let data = '';
            
            response.on('data', chunk => {
                data += chunk;
            });
            
            response.on('end', () => {
                try {
                    const parsed = JSON.parse(data);
                    console.log('API Response:', parsed);  // Log the response
                    
                    if (parsed.status === 'error') {
                        reject(new Error(parsed.message));
                    } else {
                        resolve(parsed.articles || []);
                    }
                } catch (e) {
                    reject(e);
                }
            });
        });

        req.on('error', error => {
            console.error('Request error:', error);
            reject(error);
        });

        req.end();
    });
}

async function processArticle(article) {
    const item = {
        id: Date.now() + '-' + Math.random().toString(36).substr(2, 9),
        timestamp: new Date().toISOString(),
        title: article.title || 'Untitled',
        content: article.content || article.description || '',
        source: article.source?.name || 'Unknown',
        url: article.url || '',
        ttl: Math.floor(Date.now() / 1000) + (7 * 24 * 60 * 60)
    };

    console.log('Processing:', item.title);

    await dynamodb.send(new PutCommand({
        TableName: 'prism-news',
        Item: item
    }));

    await eventbridge.send(new PutEventsCommand({
        Entries: [{
            Source: 'prism.news',
            DetailType: 'news.ingested',
            Detail: JSON.stringify(item)
        }]
    }));

    return item;
}
