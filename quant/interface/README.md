## 量化策略API

从软件架构角度来看,量化交易平台是容器,量化策略运行在量化交易平台之上,量化交易平台为量化策略提供底层服务. 
虽然从技术角度来说量化策略可以任意调用量化交易平台中的任意代码,但是从软件架构角度来说量化策略不应该随便调用量化交易平台中的代码,而是应该通过 
量化交易平台为量化策略提供的API与量化交易平台进行交互,这样才能符合基本的软件设计规范,方便未来扩展,而本目录存放的就是为量化策略提供的部分API, 
model_api用于一般策略,而datamatrix_api用于数据矩阵策略,ah_math为策略提供若干数学方法,未来将会进一步丰富各种数学方法,比如金融时间序列分析等, 
同时也会添加技术指标分析库等.