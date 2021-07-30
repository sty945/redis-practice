from pprint import pprint
from time import time
from redis import Redis

ONE_WEEK_IN_SECONDS = 7 * 86400                 
VOTE_SCORE = 432  
ARTICLES_PER_PAGE = 25

def post_article(conn: Redis, user: str, title: str, link: str):
    """发布文章

    Args:
        conn (Redis): [客户端]]
        user (str): [用户]
        title (str): [标题]
        link (str): [链接]

    Returns:
        [type]: [文章标识]
    """
    article_id = str(conn.incr('article:'))
    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)
    now = time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1
    })

    conn.zadd('score:', {article: now + VOTE_SCORE})
    conn.zadd('time:', {article: now})

    return article_id


def article_vote(conn: Redis, user: str, article: str):
    """文章投票

    Args:
        conn (Redis): [redis链接]
        user (str): [用户]
        article (str): [文章标识]
    """
    cutoff = time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return
    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        # 更新文章评分：为'score:' 中的 'article' 增加 VOTE_SCORE
        conn.zincrby('score:', VOTE_SCORE, article)
        # 更新文章投票数量：为 article 中 'votes' 值增加 1
        conn.hincrby(article, 'votes', 1)


def get_articles(conn: Redis, page: int, order='score:'):
    """获取文章分页信息，默认按照分值取出数据

    Args:
        conn (Redis): [Redis客户端]
        page (int): [页数]
        order (str, optional): [排序方式]. Defaults to 'score:'.

    Returns:
        [type]: [description]
    """
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1
    
    ids = conn.zrevrange(order, start, end)

    articles = []
    
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)   
    return articles


def add_remove_groups(conn: Redis, article_id: str, to_add: list = [], to_remove: list = []):
    """对文章进行移入、移出分组操作

    Args:
        conn (Redis): [Redis客户端]
        article_id (str): [文章id]
        to_add (list, optional): [要移入的分组]. Defaults to [].
        to_remove (list, optional): [要移出的分组]. Defaults to [].
    """
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)
    


def get_group_articles(conn: Redis, group: str, page: int, order: str = 'score:'):
    """获取分组文章

    Args:
        conn (Redis): [Redis客户端]
        group (str): [分组名称]]
        page (int): [页数]
        order (str, optional): [排序规则]. Defaults to 'score:'.

    Returns:
        [type]: [description]
    """
    key = order + group
    if not conn.exists(key):
        # 对（'group:' + group） 和 (order)取交集，其中order中取最大值作为最后的值
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)
    

def main():
    conn = Redis(host='127.0.0.1', port=6379, encoding='utf8', decode_responses=True)
    article_id = str(post_article(conn, 'username', 'A_title', 'www.baidu.com'))

    article = conn.hgetall('article:' + article_id)
    article_vote(conn, 'other_user', 'article:' + article_id)
    print("voted for the article, it now has votes:", end=' ')
    v = int(conn.hget('article:' + article_id, 'votes'))
    print(v)
    articles = get_articles(conn, 1)
    pprint(articles)
    add_remove_groups(conn, article_id, ['new-group'])
    articles = get_group_articles(conn, 'new-group', 1)
    print("articles are: \n")
    pprint(articles)
    to_del = (
        conn.keys('time:*') + conn.keys('voted:*') + conn.keys('score:*') + 
        conn.keys('article:*') + conn.keys('group:*')
    )
    print(to_del)

    print(conn.hgetall('article:1'))
    print(conn.keys('article*'))
    # if to_del:
    #     conn.delete(*to_del))


if __name__ == '__main__':
    main()
    