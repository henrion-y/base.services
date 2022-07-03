package ac_automaton

import (
	"testing"
)

func TestAcAutoMachine_Query(t *testing.T) {
	ac := NewAcAutoMachine()
	ac.AddPattern("花儿")
	ac.AddPattern("这样")
	ac.AddPattern("红")
	ac.Build()

	text := `律师代理某某参与某某诉某某某离婚纠纷再审案 | 中国法律服务网 | 【案情简介】

	某某和某某某原为夫妻，2009年12月，因感情破裂，某某将某某某起诉到北京市昌平区人民法院，要求与被告离婚并分割夫妻共同财产和共同债务。原告某某称，其与被告产生家庭矛盾已经很久，被告不信任原告、无端猜疑，二者之间的感情无以维系，确已破裂；被告某某某称，原告所述与事实不符，但同意离婚，财产要求按照2008年3月15日两者达成的财产分割协议进行分割。一审法院经审理认为，因为原被告双方均坚持离婚，因此准许二者离婚；在共同财产分割方面，将位于昌平区昌平镇的两处房屋（楼房）和位于昌平区城北街道的四处房屋（平房）判归被告某某某所有，将位于昌平区城北街道的两处房屋（平房）判归原告某某所有；在共同债务分割方面，将十九万三千元的共同债务，判决由原告某某负担八万元，由被告某某某负担十一万三千元。


	一审判决下达后，一审原告某某对判决结果不服，上诉到北京市第一中级人民法院，2010年4月，二审法院对这一案件进行了开庭审理。二审中，原告认为，离婚协议的真实性存疑，要求撤销一审法院判决的第二、三项。被告同意一审法院判决，不同意原告上诉请求。二审法院审理认为，由于原告某某未提交新的证据证明离婚协议存在瑕疵，故其主张不予采纳，因此驳回上诉，维持原判。


	二审判决下达后，某某委托北京市盈科律师事务所律师代理其再审事宜，某某详细的介绍了案件情况。律师在了解了案件详情之后，为某某诉某某某离婚纠纷一案的再审做了详细的策略谋划，并成功促使北京市第一中级人民法院启动了再审程序。


	2014年4月该案再审开庭，在庭审过程中，某某委托律师提出：一审法院认定事实不清，首先，一审判决中所分割的部分房产本就属于某某父母所有，并非夫妻共同财产；其次，某某和某某某达成的离婚协议本属于附条件的离婚协议，其协议中有关于涉案房屋的分配方式，法院引用协议内容认定房产的分割方式，又未按照协议约定处分某某父母所有的部分财产，本身存在矛盾。因此，请求法院撤销一审判决中的第二、三项以及二审判决，并由原审被上诉人承担原一、二审诉讼费用。被告某某某要求维持原判。经再审法院审理，认为原审判决认定基本事实不清，撤销原判，财产部分发回重审。


	时隔五年，案件出现转机。2015年9月某某诉某某某离婚纠纷一案在北京市昌平区人民法院进行重审，并出现案情逆转。庭审过程中，某某律师主张应当由某某某赔偿某某总价值238万余元的两套楼房房产50%的金额。某某律师认为：首先，所谓分割房产的离婚协议是一个附条件生效的协议，因为条件从未具备，因此未曾生效，不应依其进行财产分割；其次，所谓“关系暧昧”并不能证明原告某某存在导致离婚的过错，不应当承担相应责任。重审法院经过事实认定与法律权衡，认可了某某律师的主张。最终判决某某某向某某给付房屋价款六十五万元。虽然，随后某某某虽又提出上诉，但未能推翻重审判决，北京市第一中级人民法院维持了原判。

【代理意见】

	1、从离婚协议的真实意思表示看


	离婚协议是原告与被告于2008年3月15日签订的，且原告直到2009年9月才提起离婚诉讼，其间已有一年半有余。这一时间内原告与被告都未到民政部门协议离婚，即未促成离婚协议生效。据此可以推断出离婚协议只是为缓解夫妻关系所写，此协议不是原告的真实意思表示。


	2、从离婚协议与本案的关联性上看


	离婚协议是夫妻双方去民政部门办理协议离婚及财产分配的必要条件，与本案诉讼离婚不具有关联性。


	3、从离婚协议的形式上看


	离婚协议最后一页只有原告的签字，缺乏另一方当事人的签字确认，合同形式不完备。


	4、从离婚协议的内容上看


	位于昌平某某街道土地的所有权及北屋所有权依法属于申请人的父母，与申请人和被申请人的夫妻共同财产无关。尽管2002年扩建东、西、南屋是申请人与被申请人共同扩建，但是至离婚时两人已经免费居住近7年，其价值也远远比不上土地价值的收益，且其二人还将房屋出租他人收取房租，即不存在申请人父母对其折价补偿的问题。所以，后来扩建的东、西、南屋也应依法属于申请人父母。因此，离婚协议中关于原告父母所有的房屋的约定是无效的。


	5、从离婚协议的法律效力看


	本案的离婚协议实质上是一份附生效条件的合同，虽然成立但不生效，《中华人民共和国合同法》第四十五条有明确规定，即“当事人对合同的效力可以约定附条件。附生效条件的合同，自条件成就时生效，附解除条件的合同，自条件成就时失效。”原告所持离婚协议是一份成立但未生效的合同，不能作为本案的证据。2011年8月13日实施的最高人民法院关于适用《中华人民共和国婚姻法》若干问题的解释（三）第十四条进一步明确，即“当事人达成的以登记离婚或者到人民法院协议离婚为条件的财产分割协议，如果双方协议离婚未成，一方在离婚诉讼中反悔的，人民法院应当认定该财产分割协议没有生效，并根据实际情况依法对夫妻共同财产进行分割。”因此，原告所持离婚协议在本案诉讼中没有法律效力，并不能作为本案的裁判依据。

【判决结果】

	二审判决：


	驳回上诉，维持原判。

【裁判文书】

	北京市第一中级人民法院（2015）一中民再终字第09464号民事判决书

【案例评析】

	本案的关键和症结所在无疑是对于离婚协议性质的认定和效力的判断。某某一案在一审、二审阶段之所以判决偏颇、有失公正，正是因为忽视了对关键证据即离婚协议的正确判断。律师接手这个案子后，敏锐的察觉出离婚协议对于本案至关重要的影响，并从多角度论证离婚协议的效力问题，最终获得了成功。那么。离婚协议的性质是怎样的，效力又如何呢？在诸如此类的离婚案件中，如何防范离婚协议为当事人带来的不利影响呢？


	最高人民法院关于适用《中华人民共和国婚姻法》若干问题的解释(二)中对离婚财产协议做了分类：（一）离婚协议中约定财产分割的内容；（二）因离婚财产分割而专门达成的协议。本案所指离婚协议应当属于前者，也即在当事人双方婚内达成的离婚协议当中存在的财产分割部分。


	关于这种协议的性质，当前主要有四种分类。


	第一种认为离婚协议是一个涉及身份关系的单一合同。因为虽然离婚协议当中约定了关于财产分割和子女抚养的相关事项，但从本质而言，其仍是对于婚姻关系进行解除的涉及人身关系的“合同”。如果离婚登记没有达成，那么该协议应当认定为未生效，除非在诉讼中存在当事人双方针对此协议进行追认的情况。


	第二种认为离婚协议是混合民事合同。这种观点从离婚协议所包含的一般内容出发，认为离婚协议当中既包含了婚姻关系解除等涉及人身关系的部分，也包括进行夫妻共同财产分割等涉及财产关系的部分，因此应当属于一种混合合同。同时该观点认为，离婚协议中所约定的涉及财产的部分应当在双方达成合意后生效，而涉及人身的部分则须在到民政部门履行法定程序后生效。这种说法显然把合同中内容进行割裂，并区别对待，使得合同生效时间无法确定和统一，不合常理，故一般不被认同。


	第三种说法认为离婚协议是附条件的民事合同。这种说法认为，离婚协议成立的前提是当事人确已达成登记离婚的事实，离婚协议方才生效。即只有在离婚已经达成的前提下，才可以按照协议中所载方式进行财产分割。


	第四种说法认为离婚协议是个复合合同。离婚协议是多个法律行为进行复合的结果，其中既包含对于婚姻关系、子女抚养关系等人身关系进行变动的内容，又包含对于夫妻共同财产、共同债务进行处分和分割的内容，因此应当属于一种复合的合同形式。


	本案当中，根据实际情形判断，将某某与某某某签订的离婚协议认定为是一种附条件的民事合同比较恰当。最高人民法院关于适用《中华人民共和国婚姻法》若干问题的解释（三）的出台，已经很好的解释了这一类协议的效力问题。其第十四条明确规定，离婚协议在离婚登记办理前未生效。在法理方面，台湾地区民法学者王泽鉴也认为：“在两愿离婚情形，夫妻一方不能以离婚契约成立，而请求对方履行特别生效要件，从而使离婚发生效力。”


	在相关风险防范方面，针对此种涉及诉前离婚协议的案件，要在以下几个方面加以注意。首先，遇到诸如本案中的情形，应当及时明确该离婚协议的效力状态，并且如果离婚协议对我方当事人不利，应及时告知当事人切莫做出任何可能导致离婚协议被追认的行为。其次，在实践中一些法官和代理律师认为离婚协议即便未生效，也可以“留用”，作为庭外调解的依据；实际上，既然离婚协议在诉讼中已不具有效力，则在调解中亦不可作为与对方谈判的筹码进行使用。再次，离婚协议也属于合同的一种形式，应当遵循合同法的基本原则，即便登记离婚后离婚协议已经生效，也并不意味着其协议内容应当被完全的执行，若其中存在违法、违背合同法基本原则、违背公序良俗的部分，则还是可以主张离婚协议无效或者部分无效。最后，如果离婚协议中涉及赠与第三人财产的条款，且该离婚协议已生效，若想对其中的赠与条款进行变更、行使任意撤销权，则需要签订协议的双方当事人同意方可变更，不可单方行使任意撤销权。

【结语和建议】

	本案从事实证据出发，深究《中华人民共和国婚姻法》对于离婚协议效力等问题的相关规定。通过对离婚协议抽丝剥茧的分析，分别从离婚协议是否为真实的意思表示、离婚协议与诉讼的关联性、离婚协议形式、离婚协议内容和离婚协议法律效力五个方面对一审、二审的审判结论进行逐一辩驳，并且尖锐的指出了购房凭证等前两审程序一直忽视的关键证据。理论结合实证，最终推翻了终审判决，使案件结果在再审阶段得以逆转。 | `

	content := text
	results := ac.Query(content)
	for _, result := range results {
		t.Log(result)
	}
}
