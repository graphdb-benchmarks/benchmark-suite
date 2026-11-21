MATCH (account:Account {id: $id})
RETURN {createTime: account.createTime, isBlocked: account.isBlocked, type: account.type}
