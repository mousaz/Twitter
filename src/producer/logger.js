module.exports = {
  createLogger: (sourceName) => {
    return {
      log: (...args) => console.log.apply(undefined, [sourceName, ...args]),
      warn: (...args) => console.warn.apply(undefined, [sourceName, ...args]),
      error: (...args) => console.error.apply(undefined, [sourceName, ...args]),
    };
  },
};
